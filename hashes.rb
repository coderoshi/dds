require 'digest/sha1'

# A basic distributed hash implementation
class NaiveHash
  def initialize(nodes, spread=16)
    @nodes, @spread = nodes, spread
    @array = Array.new(@nodes.length * @spread)
  end

  def hash(key)
    Digest::SHA1.hexdigest(key.to_s).hex
  end

  def add(node)
    @nodes << node
  end

  def remove(node)
    @nodes.delete(node)
  end

  def node(key)
    length = @nodes.length * @spread
    @nodes[(hash(key) % length) / @spread]
  end
end



# A consistent hashing ring
class ConsistentHash
  def initialize(nodes=[], replicas=1)
    @replicas = replicas
    @ring = {}
    @nodesort = []
    for node in nodes
      add(node)
    end
  end

  def hash(key)
    Digest::SHA1.hexdigest(key.to_s).hex
  end

  # Adds node and replicas to the hash ring
  def add(node)
    @replicas.times do |i|
      key = hash("#{node.to_s}:#{i}")
      @ring[key] = node
      @nodesort.push(key)
    end
    @nodesort.sort!
  end

  # Removes node and it's replicas from the hash ring
  def remove(node)
    @replicas.times do |i|
      key = hash("#{node.to_s}:#{i}")
      @ring.delete(key)
      @nodesort.delete(key)
    end
  end

  # Returns the correct node in the ring the key is hashed to
  def node(keystr)
    return nil if @ring.empty?
    key = hash(keystr)
    @nodesort.length.times do |i|
      node = @nodesort[i]
      return @ring[node] if key <= node
    end
    @ring[ @nodesort[0] ]
  end
end


SHA1BITS = 160
class PartitionedConsistentHash
  def initialize(nodes=[], partitions=32)
    # partitions must be a power of 2
    partition_pow = Math.log2(partitions)
    raise if partition_pow != partition_pow.to_i
    @partitions = partitions
    @ring = {}
    cluster(nodes)
  end

  def range(partition, power)
    (partition*(2**power)..(partition+1)*(2**power)-1)
  end

  def hash(key)
    Digest::SHA1.hexdigest(key.to_s).hex
  end

  def cluster(nodes)
    @nodes = nodes.clone.sort
    pow = SHA1BITS - Math.log2(@partitions).to_i
    @partitions.times do |i|
      @ring[range(i, pow)] = @nodes[0]
      @nodes << @nodes.shift
    end
    @nodes.sort!
  end

  def add(node)
    # every N partitions, reassign to the new nodes
    @nodes << node
    pow = SHA1BITS - Math.log2(@partitions).to_i
    (0..@partitions).step(@nodes.length) do |i|
      @ring[range(i, pow)] = node
    end
  end

  # Returns the correct node in the ring the key is hashed to
  def node(keystr)
    return nil if @ring.empty?
    key = hash(keystr)
    @ring.each do |range, node|
      return node if range.cover?(key)
    end
  end
end



### run tests
if __FILE__ == $0

  puts "# NaiveHash"
  h = NaiveHash.new(["A", "B", "C"])
  puts h.node("foo")
  h.add("D")
  puts h.node("foo")

  h = NaiveHash.new(("A".."J").to_a, 1<<20)
  elements = 100000
  nodes = Array.new(elements)
  elements.times do |i|
    nodes[i] = h.node(i)
  end
  puts "add K"
  h.add("K")
  misses = 0
  elements.times do |i|
    misses += 1 if nodes[i] != h.node(i)
  end
  puts "misses: #{(misses.to_f/elements) * 100}%\n\n"



  puts "# ConsistentHash"
  ch = ConsistentHash.new(["A", "B", "C"])
  puts ch.node("foo")
  ch.add("D")
  puts ch.node("foo")

  h = ConsistentHash.new(("A".."J").to_a)
  elements = 100000
  nodes = Array.new(elements)
  elements.times do |i|
    nodes[i] = h.node(i)
  end
  puts "add K"
  h.add("K")
  misses = 0
  elements.times do |i|
    misses += 1 if nodes[i] != h.node(i)
  end
  puts "misses: #{(misses.to_f/elements) * 100}%\n"




  puts "# PartitionedConsistentHash"
  h = PartitionedConsistentHash.new(("A".."C").to_a, 32)
  puts h.node("foo")
  h.add("D")
  puts h.node("foo")

  h = PartitionedConsistentHash.new(("A".."J").to_a)
  elements = 100000
  nodes = Array.new(elements)
  elements.times do |i|
    nodes[i] = h.node(i)
  end
  puts "add K"
  h.add("K")
  misses = 0
  elements.times do |i|
    misses += 1 if nodes[i] != h.node(i)
  end
  puts "misses: #{(misses.to_f/elements) * 100}%\n"

end
