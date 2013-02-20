require 'json'
require './hash'

# an object to store in a server node
class NodeObject
  attr :value
  def initialize(value)
    @value = value
  end

  def to_s
    {:value=>value}.to_json
  end

  # takes a string and creates a NodeObject
  def self.deserialize(serialized)
    data = JSON.parse(serialized)
    NodeObject.new(data['value'])
  end
end


# Manages a hash ring as well as a hash of data
class Node
  attr :name
  def initialize(name, nodes=[], partitions=32)
    @name = name
    @data = {}
    @ring = PartitionedConsistentHash.new(nodes+[name], partitions)
  end

  def put(key, value)
    node = @ring.node(key)
    # store here if this is the correct node
    if node == @name
      puts "put #{key} #{value}"
      @data[@ring.hash(key)] = [NodeObject.new(value)]
    else
      nil
    end
  end

  def get(key)
    node = @ring.node(key)
    # get here if this is the correct node
    if node == @name
      puts "get #{key}"
      @data[@ring.hash(key)]
    else
      nil
    end
  end

  def close
    exit!
  end
end


# Here we have a collection of multiple
# nodes. each node has only a certain
# subset of values.
begin
  # 3 nodes
  nodea = Node.new('A', ['A', 'B', 'C'])
  nodeb = Node.new('B', ['A', 'B', 'C'])
  nodec = Node.new('C', ['A', 'B', 'C'])

  nodea.put("foo", "bar")
  p nodea.get('foo')  # nil

  nodeb.put("foo", "bar")
  p nodeb.get("foo")  # "bar"

  # 10 nodes
  names = ("A".."J").to_a
  nodes = names.map{|name| Node.new(name, names) }
  nodes.each do |node|
    node.put("foo", "bar")
    if value = node.get('foo')
      puts "#{node.name} #{value}"
    end
  end
end


# here's the crazy thing... every node knows which other node contains
# the correct value. So if a single node doesn't have it, the best thing
# it can do is pass that request onto the correct node and return the result.
