require 'zmq'
require './threads'
include Threads

ctx = ZMQ::Context.new

puts "Values in 1 Node"
start = Time.now
(0...1000).step(50) do |i|
  50.times do |j|
    thread do
      req = ctx.socket(ZMQ::REQ)
      req.connect("tcp://127.0.0.1:2200")
      req.send("put 1 key#{i+j} value#{i+j}") && req.recv
      req.close
    end
  end
  join_threads
end
puts "#{Time.now - start} secs"

puts "Values in 2 Nodes"
start = Time.now
(0...1000).step(50) do |i|
  50.times do |j|
    thread do
      req = ctx.socket(ZMQ::REQ)
      req.connect("tcp://127.0.0.1:2200")
      req.send("put 1 key#{i+j} value#{i+j}") && req.recv
      req.close
    end
  end
  join_threads
end
puts "#{Time.now - start} secs"

# If you shut down one node and run a get,
# all values should still be available

req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")
puts "Getting Values in 1 Node"
start = Time.now
1000.times do |i|
  req.send("get 1 key#{i}") && req.recv
end
puts "#{Time.now - start} secs"

puts "Getting Values in 2 Nodes"
start = Time.now
1000.times do |i|
  req.send("get 2 key#{i}") && req.recv
end
puts "#{Time.now - start} secs"

req.close
