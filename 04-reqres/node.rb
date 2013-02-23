require 'json'
require 'zmq'
require './hash'
require './threads'
require './config'
require './services'


# an object to store in a server node
class NodeObject
  attr :value
  def initialize(value)
    @value = value
  end

  def to_s
    {:value=>value}.to_json
  end

  def self.deserialize(serialized)
    data = JSON.parse(serialized)
    NodeObject.new(data['value'])
  end
end


# Manages a hash ring as well as a hash of data
class Node
  include Threads
  include Configuration
  include Services

  def initialize(name, nodes=[], partitions=32)
    @name = name
    @ring = PartitionedConsistentHash.new(nodes, partitions)
    @data = {}
  end

  def start()
    service( config("port") )
    puts "#{@name} started"
    join_threads()
  end

  def stop
    exit!
  end

  def put(socket, payload)
    key, value = payload.split(' ', 2)
    socket.send( do_put(key, value).to_s )
  end

  def get(socket, payload)
    key = payload
    socket.send( do_get(key).to_s )
  end

  def do_put(key, value)
    node = @ring.node(key)
    if node == @name
      puts "put #{key} #{value}"
      @data[@ring.hash(key)] = [NodeObject.new(value)]
    else
      remote_call(node, "put #{key} #{value}")
    end
  end

  def do_get(key)
    node = @ring.node(key)
    if node == @name
      puts "get #{key}"
      @data[@ring.hash(key)]
    else
      remote_call(node, "get #{key}")
    end
  end

  def remote_call(remote, message)
    puts "#{remote} <= #{message}"
    req = connect(ZMQ::REQ, config("port", remote), config("ip", remote))
    resp = req.send(message) && req.recv
    req.close
    resp
  end
end



begin
  name = ARGV.first
  $node = Node.new(name, ['A','B','C'])
  trap("SIGINT") { $node.stop }
  $node.start()
end