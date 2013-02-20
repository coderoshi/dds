require 'digest/sha1'

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




### run tests
if __FILE__ == $0

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

end