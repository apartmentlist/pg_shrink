module PgShrink
  require 'pg'
  require 'sequel'
  class Database::Postgres < Database


    attr_accessor :connection
    DEFAULT_OPTS = {
      postgres_url: nil,
      host: 'localhost',
      port: nil,
      username: 'postgres',
      password: nil,
      database: 'test',
      batch_size: 10000
    }.freeze

    def connection_string
     if @opts[:postgres_url]
       @opts[:postgres_url]
     else
       str = "postgres://#{@opts[:user]}"
       str << ":#{@opts[:password]}" if @opts[:password]
       str << "@#{@opts[:host]}"
       str << ":#{@opts[:port]}" if @opts[:port]
       str << "/#{@opts[:database]}"
     end
    end

    def batch_size
      @opts[:batch_size]
    end

    def initialize(opts)
      @opts = DEFAULT_OPTS.merge(opts.symbolize_keys)
      @connection = Sequel.connect(connection_string)
    end

    # WARNING!  This assumes the database is not changing during run.  If
    # requirements change we may need to insert a lock.
    def records_in_batches(table_name)
      table = self.table(table_name)
      primary_key = table.primary_key
      max_id = self.connection["select max(#{primary_key}) from #{table_name}"].
                    first[:max]
      i = 1;
      while i < max_id  do
        sql = "select * from #{table_name} where " +
                 "#{primary_key} >= #{i} and #{primary_key} < #{i + batch_size}"
        batch = self.connection[sql].all
        yield(batch)
        i = i + batch_size
      end
    end

    def update_records(table_name, old_records, new_records)
      table = self.table(table_name)
      primary_key = table.primary_key

      old_records_by_key = old_records.index_by {|r| r[primary_key]}
      new_records_by_key = new_records.index_by {|r| r[primary_key]}

      if (new_records_by_key.keys - old_records_by_key.keys).size > 0
        raise "Bad voodoo!  New records have primary keys not in old records!"
      end

      deleted_record_ids =  old_records_by_key.keys - new_records_by_key.keys
      if deleted_record_ids.any?
        raise "Bad voodoo!  Some records missing in new records!"
      end

      # TODO:  This can be optimized if performance is too slow.  Will impact
      # the speed of sanitizing the already-filtered dataset.
      new_records.each do |rec|
        if old_records_by_key[rec[primary_key]] != rec
          self.connection.from(table_name).
               where(primary_key => rec[primary_key]).
               update(rec)
        end
      end
    end

    def get_records(table_name, opts)
      self.connection.from(table_name).where(opts).all
    end

    def delete_records(table_name, condition_to_delete)
      self.connection.from(table_name).where(condition_to_delete).delete
    end
  end
end
