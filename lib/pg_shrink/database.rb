module PgShrink
  class Database
    def tables
      @tables ||= {}
    end

    # get_table should return a unique table representation for this database.
    def get_table(table_name)
      self.tables[table_name] ||= Table.new(table_name, self)
    end

    # records_in_batches should yield a series of batches # of records.
    def records_in_batches(table_name)
      raise "implement in subclass"
    end

    # The update_records method takes a set of original records and a new
    # set of records.  It deletes any records that were in the original set but
    # not the new set, and does any updates necessary between the new and old
    # set.
    def update_records(table_name, old_records, new_records)
      raise "implement in subclass"
    end
  end
end
