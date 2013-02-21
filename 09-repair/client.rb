require 'json'
require 'zmq'

ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")
req.send("put 0 foo {\"B\":1} baz") && req.recv
req.close

req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2201")
req.send("put 0 foo {} qux") && req.recv

# trigger read repair
puts req.send("get 2 foo") && req.recv

sleep 1

# read repair should be complete
puts req.send("get 2 foo") && req.recv

req.close
