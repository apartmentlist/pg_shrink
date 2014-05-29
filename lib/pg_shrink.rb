require "uri"
require "active_support"
require "active_support/inflector"
require 'active_support/core_ext/enumerable'
require 'active_support/core_ext/hash'
require "pg_shrink/version"
require "pg_shrink/database"
require "pg_shrink/database/postgres"
require "pg_shrink/table_filter"
require "pg_shrink/table_sanitizer"
require "pg_shrink/sub_table_operator"
require "pg_shrink/sub_table_filter"
require "pg_shrink/sub_table_sanitizer"
require "pg_shrink/table"
module PgShrink

  def self.blank_options
    {
      url: nil,
      config: 'Shrinkfile',
      force: false
    }
  end

  # TODO:  Some checking on format.
  def self.valid_pg_url?(url)
    uri = URI.parse(url)
    uri.scheme == 'postgres' && !uri.user.blank? && uri.path != '/'
  rescue => ex
    false
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

    # TODO: Figure out how to write a spec for this.
    unless options[:force] == true
      puts 'WARNING:  pg_shrink is destructive!  It will change this database in place.'
      puts 'Are you sure you want to continue? (y/N)'
      cont = gets
      cont = cont.strip
      unless cont == 'y' || cont == 'Y'
        puts 'Aborting!'
        exit
      end
    end

    database.shrink!
  end
end
