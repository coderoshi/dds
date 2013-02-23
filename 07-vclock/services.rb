# There's hacky a bit of metaprogramming here
# just to simplify the code.
# service Binds or Connects a Reply socket to a port.
# Whenever the socket receives a message,
# the first word is assumed to be a method, and
# the remainder of the message is a payload,
# calling and passed to the method
module Services
  def connect(kind, port=2200, ip='127.0.0.1')
    ctx = ZMQ::Context.new
    sock = ctx.socket(kind)
    sock.connect("tcp://#{ip}:#{port}")
    sock
  end

  # helper function to create a req/res service,
  # and relay message to corresponding methods
  def service(port, sink=true)
    thread do
      ctx = ZMQ::Context.new
      rep = ctx.socket(ZMQ::REP)
      uri = "tcp://127.0.0.1:#{port}"
      sink ? rep.bind(uri) : rep.connect(uri)
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
