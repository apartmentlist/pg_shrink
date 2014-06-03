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
      unless primary_key
        raise "Error:  Records in batches called on table without a primary key"
      end
      max_id = self.connection["select max(#{primary_key}) from #{table_name}"].
                    first[:max]
      i = 0;
      while i < max_id  do
        sql = "select * from #{table_name} where " +
                 "#{primary_key} > #{i} limit #{batch_size}"
        batch = self.connection[sql].all.compact

        yield(batch)
        i = batch.last[primary_key]
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

    def delete_records(table_name, conditions, exclude_conditions = [])
      query = self.connection.from(table_name)
      exclude_conditions = [exclude_conditions].flatten
      [conditions].flatten.compact.each do |cond|
        query = query.where(cond)
      end
      [exclude_conditions].flatten.compact.each do |exclude_cond|
        query = query.exclude(exclude_cond)
      end
      query.delete
    end

    def propagate_delete(opts)
      # what we conceptually want to do is delete the left outer join where id is null.
      # That's not working in postgres, so we instead use where not exists.  Docs
      # indicate using where not exists and select 1 in this case.
      # See:
      # http://www.postgresql.org/docs/current/interactive/functions-subquery.html#FUNCTIONS-SUBQUERY-EXISTS
      query = "DELETE FROM #{opts[:child_table]} WHERE NOT EXISTS (" +
                "SELECT 1 from #{opts[:parent_table]} where " +
                "#{opts[:child_table]}.#{opts[:child_key]} = " +
                "#{opts[:parent_table]}.#{opts[:parent_key]}" +
              ")"


      # Outside of the join statements, we want to maintain the ease of hash-based
      # conditions.  Do this by using a query builder but then swapping in delete SQL
      # in the end.
      query_builder = connection.from(opts[:child_table])
      [opts[:conditions]].flatten.compact.each do |cond|
        query_builder = query_builder.where(cond)
      end
      [opts[:exclude]].flatten.compact.each do |exclude_cond|
        query_builder = query_builder.exclude(exclude_cond)
      end
      sql = query_builder.sql.gsub("WHERE", "AND").
                              gsub("SELECT * FROM \"#{opts[:child_table]}\"",
                                   query)

      connection[sql].delete
    end
  end
end
