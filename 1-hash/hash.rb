require 'digest/sha1'

# A basic distributed hash implementation
class NaiveHash
  def initialize(nodes, spread=16)
    @nodes, @spread = nodes, spread
    @array = Array.new(@nodes.length * @spread)
  end

  def hash(key)
    Digest::SHA1.hexdigest(key.to_s).hex
  end

  def add(node)
    @nodes << node
  end

  def remove(node)
    @nodes.delete(node)
  end

  def node(key)
    length = @nodes.length * @spread
    @nodes[(hash(key) % length) / @spread]
  end
end




### run tests
if __FILE__ == $0

  puts "# NaiveHash"
  h = NaiveHash.new(["A", "B", "C"])
  puts h.node("foo")
  h.add("D")
  puts h.node("foo")

  h = NaiveHash.new(("A".."J").to_a, 1<<20)
  elements = 100000
  nodes = Array.new(elements)
  elements.times do |i|
    nodes[i] = h.node(i)
  end
  puts "add K"
  h.add("K")
  misses = 0
  elements.times do |i|
    misses += 1 if nodes[i] != h.node(i)
  end
  puts "misses: #{(misses.to_f/elements) * 100}%\n\n"

end