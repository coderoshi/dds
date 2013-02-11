require 'rubygems'
require 'zmq'

# http://zguide.zeromq.org/page:all#Plugging-Sockets-Into-the-Topology

# WORKS!
port = 4000
ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:#{port}")
1000.times do |i|
  req.send("put #{i}key #{i}") && req.recv
end
# req.send("put key hello") && req.recv
# p req.send("get key") && req.recv
# req.send("put foo goodbye") && req.recv

req.send("mr map{|k,v| [v]}; reduce{|vs| vs.length}")
p req.recv

req.close
