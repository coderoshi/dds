require 'zmq'

ctx = ZMQ::Context.new
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")

puts "Inserting Values"
1000.times do |i|
  req.send("put key#{i} value#{i}") && req.recv
end

puts "Getting Values"
1000.times do |i|
  puts req.send("get key#{i}") && req.recv
end

req.close