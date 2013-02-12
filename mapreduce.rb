
class Map
  def initialize(func_str, data)
    @data = data
    @func = func_str
  end

  def call
    eval(@func, binding)
  end

  # calls given map block for every value
  def map
    @results = []
    @data.each {|k,v| @results += yield k,v }
    @results
  end
end

class Reduce
  def initialize(func_str, results)
    @results = results
    @func = func_str
  end

  def call
    eval(@func, binding)
  end

  # calls given reduce block for every value
  def reduce
    yield @results
  end
end



module Mapreduce
  def map(socket, payload)
    socket.send( Map.new(payload, @data).call.to_s )
  end

  def mr(socket, payload)
    map_func, reduce_func = payload.split(/\;\s+reduce/, 2)
    reduce_func = "reduce#{reduce_func}"
    socket.send( Reduce.new(reduce_func, remote_maps(map_func)).call.to_s )
  end

  # run in parallel, then join results
  def remote_maps(map_func)
    workers, results = [], []
    @nodes.each do |node|
      workers << Thread.new do
        res = remote_call(node, "map #{map_func}")
        results += eval(res)
      end
    end
    workers.each{|w| w.join}
    results
  end
end
