require 'zmq'
require './threads'
include Threads

trap("SIGINT") { exit! }

$port = 2200

thread do   # server
  ctx = ZMQ::Context.new
  rep = ctx.socket(ZMQ::REP)
  rep.bind( "tcp://127.0.0.1:#{$port}" )
  while line = rep.recv
    msg, payload = line.split(' ', 2)
    if msg == "get"
      rep.send("Called 'GET' with #{payload}")
    elsif msg == "put"
      rep.send("Called 'PUT' with #{payload}")
    end
  end
end

thread do   # client
  ctx = ZMQ::Context.new
  req = ctx.socket(ZMQ::REQ)
  req.connect( "tcp://127.0.0.1:#{$port}" )
  req.send( "put foo bar" )
  puts req.recv
  sleep 1
  puts req.send( "put foo2 bar2" ) && req.recv
  exit!
end

join_threads
