module PgShrink
  class SubTableFilter < SubTableOperator

    def propagate_table!
      primary_key = @opts[:primary_key]
      foreign_key = @opts[:foreign_key]
      where_clause = @opts[:where]

      self.database.log('Beginning subtable propagation from ' +
               "#{self.parent.table_name} to #{self.table.table_name}")

      self.database.propagate_delete(parent_table: self.parent.table_name,
                                     child_table: self.table.table_name,
                                     parent_key: primary_key,
                                     child_key: foreign_key,
                                     where: where_clause)

      self.database.log('Done with subtable propagation from ' +
               "#{self.parent.table_name} to #{self.table.table_name}")

      if self.table.subtable_filters.any?
        self.database.vacuum_and_reindex!(self.table.table_name)
        self.table.subtable_filters.each(&:propagate_table!)
      end
    end

  end
end
