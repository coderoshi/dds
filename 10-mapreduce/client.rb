require 'json'
require 'zmq'

ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")

1000.times do |i|
  req.send("put 2 key#{i} {} #{i}") && req.recv
end

puts "Running MapReduce"
req.send("mr map{|k,v| [1]}; reduce{|vs| vs.length}")
puts req.recv
req.send("mr map{|k,v| [v.first.value.to_i]}; reduce{|vs| vs.inject(0){|s,t|s+t} }")
puts req.recv

req.close
