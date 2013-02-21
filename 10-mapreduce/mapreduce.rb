
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
    results = []
    @data.each {|k,v|
      if v.first.prime
        results += yield(k,v)
      end
    }
    results
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
    # only the data where this is the prime node
    socket.send( Map.new(payload, @data).call.to_s )
  end

  def mr(socket, payload)
    map_func, reduce_func = payload.split(/\;\s+reduce/, 2)
    reduce_func = "reduce#{reduce_func}"
    socket.send( Reduce.new(reduce_func, call_maps(map_func)).call.to_s )
  end

  # run in parallel, then join results
  def call_maps(map_func)
    results = []
    nodes = @ring.nodes - [@name]
    nodes.map {|node|
      Thread.new do
        res = remote_call(node, "map #{map_func}")
        results += eval(res)
      end
    }.each{|w| w.join}
    results += Map.new(map_func, @data).call
    results
  end
end
