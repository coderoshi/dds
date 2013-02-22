module Configuration
  def config(name, key)
    ((@configs ||= {})[name] ||= JSON::load(File.read("#{name}.json")))[key]
  end
end