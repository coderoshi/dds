module Configuration
  def config(key, name=@name)
    ((@configs ||= {})[name] ||= JSON::load(File.read("#{name}.json")))[key]
  end
end