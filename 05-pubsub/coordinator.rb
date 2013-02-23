module Coordinator

  def coordination_services(leader)
    coord_reqres = config("coord_req")
    coord_pubsub = config("coord_pub")
    
    coordinate_cluster( coord_pubsub, coord_reqres ) if leader
    track_cluster( coord_pubsub )
    inform_coordinator( 'join', coord_reqres ) unless leader
  end

  def coordinate_cluster(pub_port, rep_port)
    thread do
      ctx = ZMQ::Context.new
      pub = ctx.socket(ZMQ::PUB)
      pub.bind("tcp://*:#{pub_port}")
      rep = ctx.socket(ZMQ::REP)
      rep.bind("tcp://*:#{rep_port}")

      while line = rep.recv
        msg, node = line.split(' ', 2)
        nodes = @ring.nodes
        case msg
        when 'join'
          nodes = (nodes << node).uniq.sort
        when 'down'
          nodes -= [node]
        end
        @ring.cluster(nodes)
        # tell all nodes about the new ring
        pub.send('ring ' + nodes.join(','))
        rep.send('true')
      end
    end
  end

  def track_cluster(port)
    thread do
      sub = connect(ZMQ::SUB, port)
      sub.setsockopt(ZMQ::SUBSCRIBE, 'ring')
      # recv ONLY because it's a subscriber
      while line = sub.recv
        msg, nodes = line.split(' ', 2)
        nodes = nodes.split(',').map{|x| x.strip}
        nodes.uniq!
        @ring.cluster(nodes)
        puts "ring changed: #{@ring.nodes.inspect}"
      end
    end
  end

  def inform_coordinator(action, req_port)
    req = connect(ZMQ::REQ, req_port)
    req.send("#{action} #{@name}") && req.recv
    req.close
  end

end