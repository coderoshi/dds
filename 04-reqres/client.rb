require 'zmq'
require './threads'
include Threads

ctx = ZMQ::Context.new

puts "Inserting Values"
(0...1000).step(50) do |i|
  # throw out 50 connections at a time
  50.times do |j|
    thread do
      req = ctx.socket(ZMQ::REQ)
      req.connect("tcp://127.0.0.1:2200")
      req.send("put key#{i+j} value#{i+j}") && req.recv
      req.close
    end
  end
  join_threads
end

puts "Getting Values"
req = ctx.socket(ZMQ::REQ)
req.connect("tcp://127.0.0.1:2200")
1000.times do |i|
  puts req.send("get key#{i}") && req.recv
end
req.close