require 'digest/sha1'

SHA1BITS = 160
class PartitionedConsistentHash
  attr :nodes

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
    @nodes = nodes.clone.uniq.sort
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

  # return a list of successive nodes
  # that can also hold this value
  def pref_list(keystr, n=3)
    list = []
    key = hash(keystr)
    cover = n
    @ring.each do |range, node|
      if range === key || (cover < n && cover > 0)
        list << node
        cover -= 1
      end
    end
    list
  end
end
