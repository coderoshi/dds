require 'json'
require 'zmq'
require './hash'
require './threads'
require './config'
require './services'
require './coordinator'


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
    if Array === data
      data.map{|json| NodeObject.new(json['value'])}
    else
      NodeObject.new(data['value'])
    end
  end
end


# Manages a hash ring as well as a hash of data
class Node
  include Threads
  include Configuration
  include Services
  include Coordinator

  def initialize(name, nodes=[], partitions=32)
    @name = name
    @ring = PartitionedConsistentHash.new(nodes+[name], partitions)
    @data = {}
  end

  def start(leader)
    coordination_services( leader )
    service( config("port") )
    puts "#{@name} started"
    join_threads()
  end

  def stop
    inform_coordinator( 'down', config( "coord_req" ) )
  ensure
    exit!
  end

  def put(socket, payload)
    n, key, value = payload.split(' ', 3)
    socket.send( do_put(key, value, n.to_i).to_s )
  end

  def get(socket, payload)
    n, key = payload.split(' ', 2)
    socket.send( do_get(key, n.to_i).to_s )
  end

  def do_put(key, value, n=1)
    # 0 means insert locally
    if n == 0
      puts "put 0 #{key} #{value}"
      @data[@ring.hash(key)] = [NodeObject.new(value)]
    elsif @ring.pref_list(key, n).include?(@name)
      puts "put #{n} #{key} #{value}"
      @data[@ring.hash(key)] = [NodeObject.new(value)]
      replicate("put 0 #{key} #{value}", key, n)
      @data[@ring.hash(key)]
    else
      remote_call(@ring.node(key), "put #{n} #{key} #{value}")
    end
  end

  def do_get(key, n=1)
    if n == 0
      puts "get 0 #{key}"
      @data[@ring.hash(key)] || 'null'
    # rather than checking if this is the correct node,
    # check that this node is in the pref list.
    # if not, pass it through to the proper node
    elsif @ring.pref_list(key, n).include?(@name)
      puts "get #{n} #{key}"
      results = replicate("get 0 #{key}", key, n)
      results.map! do |r|
        r == 'null' ? nil : NodeObject.deserialize(r)
      end
      results << @data[@ring.hash(key)]
      results.flatten!
      results.uniq! {|o| o && o.value }
      results.compact!
      results
    else
      results = remote_call(@ring.node(key), "get #{n} #{key}")
      NodeObject.deserialize(results)
    end
  end

  def replicate(message, key, n)
    list = @ring.pref_list(key, n)
    list -= [@name]
    results = []
    while replicate_node = list.shift
      results << remote_call(replicate_node, message)
    end
    results
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
  name, leader = ARGV
  leader = !leader.nil?
  $node = Node.new(name)
  trap("SIGINT") { $node.stop }
  $node.start(leader)
end