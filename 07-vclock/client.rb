require 'json'
require 'zmq'

ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")

# create a conflict with vclocks
req.send("put 1 foo {\"B\":1} hello1") && req.recv
req.send("put 1 foo {\"C\":1} hello2") && req.recv
puts req.send("get 2 foo") && req.recv

sleep 5

# resolve the conflict by decending from one of the vclocks
req.send("put 2 foo {\"B\":3} hello1") && req.recv
puts req.send("get 2 foo") && req.recv

req.close
