module PgShrink
  class Database
    def tables
      @tables ||= {}
    end

    # get_table should return a unique table representation for this database.
    def get_table(table_name)
      self.tables[table_name] ||= Table.new(self, table_name)
    end

    def filter_table(table_name, opts = {})
      table = self.get_table(table_name)
      table.set_opts(opts)
      yield table
    end

    # records_in_batches should yield a series of batches # of records.
    def records_in_batches(table_name)
      raise "implement in subclass"
    end

    # get_records should take a table name and options hash and return a specific set of records
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

    def run_filters
      self.tables.values.each do |table|
        table.run_filters
      end
    end
  end
end
