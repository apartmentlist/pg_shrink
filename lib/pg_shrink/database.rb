module PgShrink
  class Database
    def tables
      @tables ||= {}
    end

    # table should return a unique table representation for this database.
    def table(table_name)
      tables[table_name] ||= Table.new(self, table_name)
    end

    def filter_table(table_name, opts = {})
      table = self.table(table_name)
      # we want to allow composability of filter specifications, so we always
      # update existing options rather than overriding
      table.update_options(opts)
      yield table if block_given?
    end

    def remove_table(table_name, opts = {})
      table = self.table(table_name)
      table.update_options(opts)
      table.mark_for_removal!
    end

    # records_in_batches should yield a series of batches # of records.
    def records_in_batches(table_name)
      raise "implement in subclass"
    end

    # get_records should take a table name and options hash and return a
    # specific set of records
    def get_records(table_name, opts)
      raise "implement in subclass"
    end

    # The update_records method takes a set of original records and a new
    # set of records.  It should throw an error if there are any records missing,
    # so it should not be used for deletion.
    def update_records(table_name, old_records, new_records)
      raise "implement in subclass"
    end

    # The delete_records method takes a table name and a condition to delete on.
    def delete_records(table_name, conditions, exclude_conditions = nil)
      raise "implement in subclass"
    end

    # vacuum and reindex is pg specific... do nothing in other cases
    def vacuum_and_reindex(table_name)
    end

    # This is kind of a leaky abstraction b/c I'm not sure how this would work
    # outside of sql
    def propagate_delete(opts)
      raise "implement in subclass"
    end

    def filter!
      tables.values.each(&:filter!)
    end

    def sanitize!
      tables.values.each(&:sanitize!)
    end

    def shrink!
      filter!
      sanitize!
    end

    def initialize(opts = {})
      @opts = opts
    end

    def log(message)
      if @opts[:log]
        puts "#{Time.now}: #{message}"
      end
    end
  end
end
