require 'rubygems'
require 'zmq'
require 'json'

# http://zguide.zeromq.org/page:all#Plugging-Sockets-Into-the-Topology

# How do we live beyond node failure? N Replication
# How do we know which value is correct? Add Vector Clocks


# Use PUSH/PULL for AAE
# Map/Reduce should be the last thing
# TODO: Make add-ons modules

# TODO:
# Replace pub/sub ring-leader with req/res gossip protocol
# Allow sloppy quorums and data transfer of downed nodes with hinted handoff

port = 4000
ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:#{port}")
puts "Inserting Values"
# 1000.times do |i|
#   req.send("put #{i}key #{i}") && req.recv
# end

# VC1
# resp = req.send("put 2 key {} hello") && req.recv

# VC2 - conflict!
# req.send("put 0 key {\"A:2201\":1} hello1") && req.recv
# req.send("put 0 key {\"B:2201\":1} hello2") && req.recv
# puts req.send("get 2 key") && req.recv

# VC3 - resolve!
# resp = req.send("put 2 key {\"A:2201\":2,\"B:2201\":2} hello") && req.recv

# Counters
req.send("put 1 key {\"A:2201\":1} +1") && req.recv
req.send("put 1 key {\"B:2202\":1} +1") && req.recv
vals = req.send("get 2 key") && req.recv
p JSON.parse(vals)
p JSON.parse(vals).reduce(0){|sum,v| sum + v['value'].to_i}

# puts req.send("get 2 key") && req.recv
# p req.send("get 3 key") && req.recv
# req.send("put foo goodbye") && req.recv

# puts "Running MapReduce"
# req.send("mr map{|k,v| [v]}; reduce{|vs| vs.length}")
# p req.recv

req.close
