require 'json'
require 'zmq'

ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2201")

req.send("put_counter 1 foo +1") && req.recv
req.send("put_counter 1 foo +2") && req.recv
req.send("put_counter 2 foo +1") && req.recv
puts req.send("get_counter 2 foo") && req.recv

req.close
