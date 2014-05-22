module PgShrink
  require 'pg'
  require 'sequel'
  class Database::Postgres < Database


    attr_accessor :connection
    def default_opts
      {
       host: 'localhost',
       port: nil,
       username: 'postgres',
       password: nil,
       database: 'test'
      }
    end

    def connection_string
     str = "postgres://#{@opts[:user]}"
     str = str + ":#{@opts[:password]}" if @opts[:password]
     str = str + "@#{@opts[:host]}"
     str = str + ":#{@opts[:port]}" if @opts[:port]
     str = str + "/#{@opts[:database]}"
    end

    def batch_size
      @batch_size ||= 10000
    end

    def initialize(opts)
      @opts = default_opts.merge(opts.symbolize_keys)
      @batch_size = opts[:batch_size]
      @connection = Sequel.connect(connection_string)
    end

    def records_in_batches(table_name)
      table = self.get_table(table_name)
      primary_key = table.primary_key
      max_id = self.connection["select max(#{primary_key}) from #{table_name}"].first[:max]
      i = 1;
      while(i < max_id) do
        batch = self.connection["select * from #{table_name} where #{primary_key} >= #{i} and #{primary_key} < #{i + batch_size}"].all
        yield(batch)
        i = i + batch_size
      end
    end

    def update_records(table_name, old_records, new_records)
    end
  end
end
