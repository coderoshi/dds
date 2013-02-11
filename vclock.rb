class VectorClock
  attr_reader :vector
  def initialize(vector={})
    @vector = vector
  end

  def increment(clientId)
    count = @vector[clientId] || 0
    @vector[clientId] = count + 1
  end

  def descends_from?(vclock2)
    (self <=> vclock2) > 0 rescue false
  end

  def conflicts_with?(vclock2)
    (self <=> vclock2) rescue return true ensure false
  end

  def <=>(vclock2)
    equal, descendant, ancestor = true, true, true
    @vector.each do |cid, count|
      if count2 = vclock2.vector[cid]
        equal, descendant = false, false if count < count2
        equal, ancestor = false, false if count > count2
      elsif count != 0
        equal, ancestor = false, false
      end
    end
    vclock2.vector.each do |cid2, count2|
      equal, descendant = false, false if !@vector.include?(cid2) && count2 != 0
    end
    if equal then return 0
    elsif descendant && !ancestor then return 1
    elsif ancestor && !descendant then return -1
    end
    raise "Conflict"
  end
end

vc = VectorClock.new
vc.increment("adam")
vc.increment("barb")

vc2 = VectorClock.new(vc.vector.clone)
puts vc.descends_from?(vc2)

vc.increment("adam")
puts vc.descends_from?(vc2)

vc2.increment("barb")
puts vc2.conflicts_with?(vc)
