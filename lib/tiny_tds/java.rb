require "tiny_tds/java/version"

begin
  require 'sequel'
rescue
  STDERR.puts 'It looks like you run on jruby. Please add gem "sequel" and "jdbc-mssql-azure" to your Gemfile and use mode: :sequel in your database.yml.'
end

module TinyTds
  module Java
  end
end
