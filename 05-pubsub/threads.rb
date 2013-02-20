# Helper module to manage multiple threads
module Threads
  def thread
    @threads = [] unless defined?(@threads)
    @threads << Thread.new do
      begin
        yield
      rescue => e
        puts e.backtrace.join("\n")
      end
    end
  end

  def join_threads
    @threads.each{ |t| t.join }
  end
end
