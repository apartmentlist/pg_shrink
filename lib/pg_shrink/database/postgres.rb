module PgShrink
  require 'pg'
  require 'sequel'
  class Database::Postgres < Database


    attr_accessor :connection
    DEFAULT_OPTS = {
      host: 'localhost',
      port: nil,
      username: 'postgres',
      password: nil,
      database: 'test',
      batch_size: 10000
    }

    def connection_string
     str = "postgres://#{@opts[:user]}"
     str << ":#{@opts[:password]}" if @opts[:password]
     str << "@#{@opts[:host]}"
     str << ":#{@opts[:port]}" if @opts[:port]
     str << "/#{@opts[:database]}"
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
      max_id = self.connection["select max(#{primary_key}) from #{table_name}"].first[:max]
      i = 1;
      while i < max_id  do
        sql = "select * from #{table_name} where " +
                 "#{primary_key} >= #{i} and #{primary_key} < #{i + batch_size}"
        batch = self.connection[sql].all
        yield(batch)
        i = i + batch_size
      end
    end

    # TODO:  Do we need to do some checking to make sure new_records has
    # primary keys that are a strict subset of update_records?
    def update_records(table_name, old_records, new_records)
      table = self.table(table_name)
      primary_key = table.primary_key

      # no inject so we only create 1 object
      old_records_by_key = {}
      old_records.each {|r| old_records_by_key[r[primary_key]] = r}


      new_records_by_key = {}
      new_records.each {|r| new_records_by_key[r[primary_key]] = r}

      if (new_records_by_key.keys - old_records_by_key.keys).size > 1
        raise "Bad voodoo!  New records have primary keys not included in old records!"
      end

      deleted_record_ids =  old_records_by_key.keys - new_records_by_key.keys
      if deleted_record_ids.any?
        self.delete_records(table_name, {primary_key => deleted_record_ids})
      end

      # TODO:  Is it worth optimizing this to do bulk updates?  Or are bulk
      # deletes above good enough?
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
