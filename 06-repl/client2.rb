require 'zmq'

ctx = ZMQ::Context.new

req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")
req.send("put 0 foo bar") && req.recv
req.close

req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2201")
req.send("put 0 foo baz") && req.recv
req.close

req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2202")
req.send("put 0 foo qux") && req.recv
puts req.send("get 3 foo") && req.recv
req.close
