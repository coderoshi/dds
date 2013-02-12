require 'digest/sha1'
require 'rubygems'
require 'zmq'
require './hashes'
require './vclock'
require './merkle'
# require './mapreduce'

class NodeObject
  attr :value, :vclock
  def initialize(value, vclock)
    @value = value
    @vclock = vclock
  end

  def <=>(nobject2)
    vclock <=> nobject2.vclock
  end

  def to_s
    serialize
  end

  def serialize
    {:value=>value, :vclock=>vclock}.to_json
  end

  def self.deserialize(serialized)
    data = JSON.parse(serialized)
    vclock = VectorClock.deserialize(data['vclock'])
    NodeObject.new(data['value'], vclock)
  end
end

class Node
  # include Mapreduce

  def initialize(name, nodes=[], partitions=32)
    @name = name
    @nodes = nodes << @name
    @ring = PartitionedConsistentHash.new(@nodes, partitions)
    @data = {}
    @services = []
  end

  def start(port, coord_pubsub, leader)
    @coord_reqres = coord_pubsub + 800
    @leader = leader

    # ring coordination
    track_cluster(coord_pubsub)
    coordinate_cluster(coord_pubsub, @coord_reqres)
    inform_coordinator('join', @coord_reqres)

    # service(4001, false) #port)
    service(port)
    service(port + 1000)
    puts "service started"

    @services.each{|service| service.join}
  end

  Thread.abort_on_exception = true
  def thread
    @services << Thread.new do
      begin
        yield
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  # now that we can keep our clusters in
  # sync, how do we actually store data?
  # Note: pub/sub, by definition, creates a SPOF (the publisher)
  # so this example is a bit contrived. However, it's not as
  # terrible as it seems. Once all of the nodes know the ring
  # configuration, it doesn't matter if the publisher goes down
  # (until you need to make another ring change)
  def track_cluster(port)
    thread do
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
        puts "ring changed: #{@nodes.inspect}"
      end
    end
  end

  def coordinate_cluster(pub_port, rep_port)
    if @leader
      thread do
        ctx = ZMQ::Context.new
        pub = ctx.socket(ZMQ::PUB)
        pub.bind("tcp://*:#{pub_port}")
        rep = ctx.socket(ZMQ::REP)
        rep.bind("tcp://*:#{rep_port}")

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
      end
    end
  end

  def inform_coordinator(action, req_port)
    unless @leader
      ctx = ZMQ::Context.new
      req = ctx.socket(ZMQ::REQ)
      req.connect("tcp://127.0.0.1:#{req_port}")
      req.send("#{action} #{@name}") && req.recv
      req.close
    end
  end

  # helper function to create a req/res service,
  # and relay message to corrosponding methods
  def service(port, sink=true)
    thread do
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
        # call the function of the given message
        # warning: really unsafe, but easy!
        send( msg.to_sym, rep, payload )
      end
    end
  end

  def put(socket, payload)
    n, key, vc, value = payload.split(' ', 4)
    socket.send( do_put(key, vc, value, n.to_i).to_s )
  end

  def get(socket, payload)
    n, key, value = payload.split(' ', 3)
    socket.send( do_get(key, n.to_i).to_s )
  end

  def method_missing(method, *args, &block)
    socket, payload = args
    payload.send('what?') if payload
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
  def do_get(key, n=1)
    # 0 means get locally
    if n == 0
      puts "get 0 #{key}"
      return @data[@ring.hash(key)] || "null"
    end
    # if we ask for just one, and it's here, return it!
    if n == 1 && value = @data[@ring.hash(key)]
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
      results << @data[@ring.hash(key)]
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

  def do_put(key, vc, value, n=1)
    # 0 means insert locally
    # use the vclock given
    if n == 0
      vclock = VectorClock.deserialize(vc)
      puts "put 0 #{key} #{vclock} #{value}"
      new_obj = [NodeObject.new(value, vclock)]
      return @data[@ring.hash(key)] = new_obj
    end
    # check for correct node
    node = @ring.node(key)
    if node == @name
      # we don't care what a given vclock is,
      # just grab this node's clock as definitive
      vclock = VectorClock.deserialize(vc)
      node_object = []
      if current_obj = @data[@ring.hash(key)]
        # if no clock was given, just use the old one
        if vclock.empty?
          # if no sibling, just update
          if current_obj.length == 1
            vclock = current_obj.first.vclock
            vclock.increment(@name)
            node_object = [NodeObject.new(value, vclock)]
          else
            vclock.increment(@name)
            node_object += [NodeObject.new(value, vclock)]
          end
        # otherwise, ensure the given one is a decendant of
        # an existing clock. If not, create a sibling
        else
          if current_obj.find{|o| o.vclock.descends_from?(vclock)}
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
      puts "put #{n} #{key} #{vclock} #{value}"
      @data[@ring.hash(key)] = node_object
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
    inform_coordinator('down', @coord_reqres)
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

  coord_pubsub = coordinator
  coord_reqres = coordinator + 800

  $node.start(port.to_i, coordinator.to_i, leader)
end
