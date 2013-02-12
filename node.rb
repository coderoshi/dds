require 'digest/sha1'
require 'rubygems'
require 'zmq'
require './hashes'
require './vclock'

class Map
  def initialize(func_str, data)
    @data = data
    @func = func_str
  end

  def call
    eval(@func, binding)
  end

  # calls given map block for every value
  def map
    @results = []
    @data.each {|k,v| @results += yield k,v }
    @results
  end
end

class Reduce
  def initialize(func_str, results)
    @results = results
    @func = func_str
  end

  def call
    eval(@func, binding)
  end

  # calls given reduce block for every value
  def reduce
    yield @results
  end
end

class NodeObject
  attr :value, :vclock
  def initialize(value, vclock)
    @value = value
    @vclock = vclock
  end

  def <=>(nobject2)
    vclock <=> nobject2.vclock
  end

  def serialize
    {:value=>value, :vclock=> vclock}.to_json
  end

  def self.deserialize(serialized)
    data = JSON.parse(serialized)
    vclock = VectorClock.deserialize(data['vclock'])
    NodeObject.new(data['value'], vclock)
  end

  def to_s
    serialize
  end
end

Thread.abort_on_exception = true
class Node
  def initialize(name, nodes=[], partitions=32)
    @name = name
    @nodes = nodes << @name
    @ring = PartitionedConsistentHash.new(@nodes, partitions)
    @data = {}
    @remotes = {}
  end

  # receiving a request, forwarding
  def start(port, coordinator, leader)
    @tracker = track_cluster(coordinator)
    @leader = leader
    if leader
      @ring_coordinator = coordinate(coordinator)
    else
      join(coordinator)
    end

    @service = service(4001, false) #port)
    @inter_service = service(port.to_i + 1000)
    puts "accepting client requests"
    
    @service.join if @service
    @inter_service.join if @inter_service
    @ring_coordinator.join if @ring_coordinator
    @tracker.join if @tracker
  end

  # now that we can keep our clusters in
  # sync, how do we actually store data?
  # Note: pub/sub, by definition, creates a SPOF (the publisher)
  # so this example is a bit contrived. However, it's not as
  # terrible as it seems. Once all of the nodes know the ring
  # configuration, it doesn't matter if the publisher goes down
  # (until you need to make another ring change)
  def track_cluster(port)
    puts "tracking cluster changes"
    Thread.new do
      begin
        ctx = ZMQ::Context.new
        sub = ctx.socket(ZMQ::SUB)
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.setsockopt(ZMQ::SUBSCRIBE, 'ring')
        # recv ONLY because it's a subscriber
        while line = sub.recv
          msg, nodes = line.split(' ', 2)
          @nodes = nodes.split(',').map{|x| x.strip}
          @nodes.uniq!
          @ring.cluster(@nodes.sort!)
          puts "ring: #{@nodes.inspect}"
        end
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  # coord node?
  def join(coordinator)
    @coordinator = coordinator
    ctx = ZMQ::Context.new
    req = ctx.socket(ZMQ::REQ)
    req.connect("tcp://127.0.0.1:#{coordinator.to_i + 800}")
    req.send("join #{@name}") && req.recv
    req.close
  end

  def coordinate(port)
    Thread.new do
      begin
        ctx = ZMQ::Context.new
        pub = ctx.socket(ZMQ::PUB)
        pub.bind("tcp://*:#{port}")
        rep = ctx.socket(ZMQ::REP)
        rep.bind("tcp://*:#{port + 800}")

        while line = rep.recv
          msg, node = line.split(' ', 2)
          case msg
          when 'join'
            @nodes = (@nodes << node).uniq.sort
          when 'down'
            @nodes -= [node]
          end

          # tell all nodes about the new ring
          pub.send('ring ' + @nodes.join(','))
          rep.send('true')
        end
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  def service(port, sink=true)
    Thread.new do
      begin
        ctx = ZMQ::Context.new
        rep = ctx.socket(ZMQ::REP)
        if sink
          rep.bind("tcp://127.0.0.1:#{port}")
        else
          rep.connect("tcp://127.0.0.1:#{port}")
        end
        # recv/send because it's a req/res
        while line = rep.recv
          msg, payload = line.split(' ', 2)
          case msg
          when 'put'
            n, key, vc, value = payload.split(' ', 4)
            rep.send( put(key, vc, value, n.to_i).to_s )
          when 'get'
            n, key, value = payload.split(' ', 3)
            rep.send( get(key, n.to_i).to_s )
          when 'map'
            rep.send( Map.new(payload, @data).call.to_s )
          when 'mr'
            map_func, reduce_func = payload.split(/\;\s+reduce/, 2)
            reduce_func = "reduce#{reduce_func}"
            rep.send( reduce(reduce_func, remote_maps(map_func)).to_s )
          else
            rep.send( 'what?' )
          end
        end
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  # run in parallel, then join results
  def remote_maps(map_func)
    # TODO: is ruby array threadsafe?
    results = []
    workers = []
    @nodes.each do |node|
      workers << Thread.new do
        res = remote_call(node, "map #{map_func}")
        results += eval(res)
      end
    end
    workers.each{|w| w.join }
    results
  end

  def map(map_func, data)
    Map.new(map_func, data).call
  end

  def reduce(reduce_func, results)
    Reduce.new(reduce_func, results).call
  end

  def hash(key)
    Digest::SHA1.hexdigest(key.to_s).hex
  end

  def cluster(nodes)
    @nodes.each do |node|
      @ring.add(node)
      puts "Added #{node}"
      @nodes << node
    end
  end

  # return a list of successive nodes
  # that can also hold this value
  def pref_list(n=3)
    list = @nodes.clone
    while list.first != @name
      list << list.shift
    end
    list[1...n]
  end

  # with these messages we don't look for the
  # proper node, we just add the value
  def get(key, n=1)
    # 0 means get locally
    if n == 0
      puts "get 0 #{key}"
      return @data[hash(key)] || "null"
    end
    # if we ask for just one, and it's here, return it!
    if n == 1 && value = @data[hash(key)]
      puts "get 1 #{key} (local)"
      return value
    end
    node = @ring.node(key)
    if node == @name
      puts "get #{n} #{key}"
      results = replicate("get 0 #{key}", n)
      results.map! do |r|
        r == 'null' ? nil : NodeObject.deserialize(r)
      end
      results << @data[hash(key)]
      results.compact!
      begin
        results.sort.first
      rescue
        # a conflic forces sublings
        puts "Conflict!"
        return results
      end
    else
      remote_call(node, "get #{n} #{key}")
    end
  end

  def put(key, vc, value, n=1)
    # 0 means insert locally
    # use the vclock given
    if n == 0
      vclock = VectorClock.deserialize(vc)
      puts "put 0 #{key} #{vclock} #{value}"
      new_obj = [NodeObject.new(value, vclock)]
      return @data[hash(key)] = new_obj
    end
    # check for correct node
    node = @ring.node(key)
    if node == @name
      # we don't care what a given vclock is,
      # just grab this node's clock as definitive
      vclock = VectorClock.deserialize(vc)
      node_object = []
      if current_obj = @data[hash(key)]
        # if no clock was given, just use the old one
        if vclock.empty?
          vclock = current_obj.vclock
          vclock.increment(@name)
          node_object = [NodeObject.new(value, vclock)]
        # otherwise, ensure the given one is a decendant of
        # an existing clock. If not, create a sibling
        else
          current_obj = [current_obj] unless Array === current_obj
          if current_obj.find{|o| vclock.descends_from?(o.vclock)}
            # is a decendant, assume this is resolving a conflict
            vclock.increment(@name)
            node_object = [NodeObject.new(value, vclock)]
          else
            # not a decendant of anything, ie conflict. add as a sibling
            vclock.increment(@name)
            node_object = current_obj + [NodeObject.new(value, vclock)]
          end
        end
      else
        vclock.increment(@name)
        node_object = [NodeObject.new(value, vclock)]
      end
      # vclock = old_val = @data[hash(key)] ? old_val.vclock : VectorClock.new
      # vclock.increment(@name)
      puts "put #{n} #{key} #{vclock} #{value}"
      @data[hash(key)] = node_object
      replicate("put 0 #{key} #{vclock} #{value}", n)
      return new_obj
    else
      remote_call(node, "put #{n} #{key} #{vc} #{value}")
    end
  end

  def replicate(message, n)
    list = pref_list(n)
    results = []
    while replicate_node = list.shift
      results << remote_call(replicate_node, message)
    end
    results
  end

  def remote_call(node, message)
    puts "#{node} <= #{message}"
    name, port = node.split ':'
    inter_port = port.to_i + 1000
    ctx = ZMQ::Context.new
    req = ctx.socket(ZMQ::REQ)
    req.connect("tcp://127.0.0.1:#{inter_port}")
    req.send(message)
    resp = req.recv
    req.close
    resp
  end

  def close
    unless @leader
      ctx = ZMQ::Context.new
      req = ctx.socket(ZMQ::REQ)
      req.setsockopt(ZMQ::LINGER, 0)
      req.connect("tcp://127.0.0.1:#{@coordinator.to_i + 800}")
      req.send("down #{@name}") && req.recv
      req.close
    end
  ensure
    exit!
  end
end

# we're using ports here, because it's easier to run locally
# however, this could easily be an ip address:port
begin
  $n = 3
  node, coordinator = ARGV
  name, port = node.split ':'
  $node = Node.new(node)

  trap("SIGINT") { $node.close }

  leader = false
  if coordinator.nil?
    coordinator = port.to_i - 100
    leader = true
  end
  $node.start(port, coordinator, leader)
end
