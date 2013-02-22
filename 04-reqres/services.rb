# There's hacky a bit of metaprogramming here
# just to simplify the code.
# service Binds or Connects a Reply socket to a port.
# Whenever the socket receives a message,
# the first word is assumed to be a method, and
# the remainder of the message is a payload,
# calling and passed to the method
module Services
  def build_socket(kind, server=false, name=@name)
    ctx = ZMQ::Context.new
    sock = ctx.socket(kind)
    if server
      sock.bind("tcp://*:#{config(name,'port')}")
    else
      sock.connect("tcp://#{config(name,'ip')}:#{config(name,'port')}")
    end
    sock
  end

  # helper function to create a req/res service,
  # and relay message to corresponding methods
  def service(port)
    thread do
      ctx = ZMQ::Context.new
      rep = ctx.socket(ZMQ::REP)
      rep.bind("tcp://*:#{port}")
      # recv/send because it's a req/res
      while line = rep.recv
        msg, payload = line.split(' ', 2)
        # call the function of the given message
        # warning: really unsafe, but easy!
        send( msg.to_sym, rep, payload )
      end
    end
  end

  def method_missing(method, *args, &block)
    socket, payload = args
    payload.send('bad message') if payload
  end
end
