# gem install zmq
# gem install ffi-rzmq
# ruby pub.rb
require 'rubygems'
require 'ffi-rzmq'

# the first node is the publisher
# all future nodes are subscribers
# Clearly, master introduces a SPOF

context = ZMQ::Context.new

frontend = context.socket(ZMQ::ROUTER)
frontend.bind("tcp://*:#{4000}")

backend = context.socket(ZMQ::DEALER)
backend.bind("tcp://*:#{4001}")

puts "Starting proxy"

# message queue pipe
ZMQ::Device.new(ZMQ::QUEUE, frontend, backend)
