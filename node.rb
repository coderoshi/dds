require 'digest/sha1'
require 'rubygems'
require 'zmq'
require './hashes'

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
            @nodes << node
            @nodes.uniq!
            @nodes.sort!
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
          msg, key, value = line.split(' ', 3)
          # p line
          case msg
          when 'put'
            rep.send( put(key, value).to_s )
          when 'get'
            rep.send( get(key).to_s )
          when 'map'
            _, map_func = line.split(' ', 2)
            rep.send( Map.new(map_func, @data).call.to_s )
          when 'mr'
            map_func, reduce_func = mr_message(line)
            results = remote_maps(map_func)
            rep.send( reduce(reduce_func, results).to_s )
          else
            rep.send( 'what?' )
          end
        end
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  def mr_message(message)
    _, mr_code = message.split ' ', 2
    map_func, reduce_func = mr_code.split(/\;\s+reduce/, 2)
    # TODO: split without removing 'reduce'
    [map_func, "reduce#{reduce_func}"]
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

  def get(key)
    node = @ring.node(key)
    if node == @name
      puts "get #{key}"
      @data[hash(key)]
    else
      remote_call(node, "get #{key}")
    end
  end

  def put(key, value)
    # check for correct node
    node = @ring.node(key)
    if node == @name
      puts "put #{key} #{value}"
      @data[hash(key)] = value
      'true'
    else
      remote_call(node, "put #{key} #{value}")
    end
  end

  # TODO: test three nodes
  # TODO: keep ports open
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
  node, coordinator = ARGV
  name, port = node.split ':'
  $node = Node.new(node)

  trap("SIGINT") { $node.close }

  # TODO: gossip?

  leader = false
  if coordinator.nil?
    coordinator = port.to_i - 100
    leader = true
  end
  $node.start(port, coordinator, leader)
end
