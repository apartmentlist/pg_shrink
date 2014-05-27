require "active_support"
require "active_support/inflector"
require "pg_shrink/version"
require "pg_shrink/database"
require "pg_shrink/database/postgres"
require "pg_shrink/table_filter"
require "pg_shrink/table_sanitizer"
require "pg_shrink/sub_table"
require "pg_shrink/table"
module PgShrink

  def self.blank_options
    {
      url: nil,
      config: 'Shrinkfile'
    }
  end

  # TODO:  Some checking on format.
  def self.valid_pg_url?(url)
    url.is_a?(String)
  end

  def self.run(options)
    unless File.exists?(options[:config])
      raise "Could not find file: #{options[:config]}"
    end

    unless valid_pg_url?(options[:url])
      raise "Invalid postgres url: #{options[:url]}"
    end

    database = Database::Postgres.new(:postgres_url => options[:url])

    database.instance_eval(File.read(options[:config]), options[:config], 1)
    database.shrink!
  end
end
