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

  def self.validate_pg_url!(url)
    if url.blank?
      abort("Error loading postgres: " +
            "Please specify postgres url using -u <postgres_url>")
    end
    uri = URI.parse(url)
    if uri.scheme == 'postgres' && !uri.user.blank? && uri.path != '/'
      return true
    else
      abort("Error loading postgres: " +
            "#{url} is not a valid postgres url")
    end
  rescue => ex
    abort(
      "Error loading postgres: " +
      "#{url} is not a valid postgres url"
    )
  end

  def self.run(options)
    unless File.exists?(options[:config])
      if options[:config] == 'Shrinkfile'
        abort("Error loading Shrinkfile: " +
              "Please specify location using -c <path/to/Shrinkfile>")
      else
        abort("Error loading Shrinkfile: " +
              "Could not find file at: #{options[:config]}")
      end
    end

    validate_pg_url!(options[:url])

    database = Database::Postgres.new(:postgres_url => options[:url])

    database.instance_eval(File.read(options[:config]), options[:config], 1)

    # TODO: Figure out how to write a spec for this.
    unless options[:force] == true
      puts 'WARNING:  pg_shrink is destructive!  It will change this database in place.'
      puts 'Are you sure you want to continue? (y/N)'
      cont = gets
      cont = cont.strip
      unless cont == 'y' || cont == 'Y'
        abort('Aborting!')
      end
    end

    database.shrink!
  end
end
