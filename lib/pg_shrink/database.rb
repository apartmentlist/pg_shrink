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
      yield table
    end

    def remove_table(table_name)
      table = self.table(table_name)
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
    # set of records.  It deletes any records that were in the original set but
    # not the new set, and does any updates necessary between the new and old
    # set.
    def update_records(table_name, old_records, new_records)
      raise "implement in subclass"
    end

    def filter!
      tables.values.each do |table|
        table.filter!
      end
    end

    def sanitize!
      tables.values.each do |table|
        table.sanitize!
      end
    end

    def shrink!
      filter!
      sanitize!
    end
  end
end
