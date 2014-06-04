module PgShrink
  class SubTableFilter < SubTableOperator

    def propagate_table!
      primary_key = @opts[:primary_key]
      foreign_key = @opts[:foreign_key]
      additional_conditions = {}
      if @opts[:type_key] && @opts[:type]
        additional_conditions[@opts[:type_key]] = @opts[:type]
      end
      self.database.log("Beginning subtable propagation from " +
               "#{self.parent.table_name} to #{self.table.table_name}")
      self.database.propagate_delete(:parent_table => self.parent.table_name,
                                     :child_table => self.table.table_name,
                                     :parent_key => primary_key,
                                     :child_key => foreign_key,
                                     :conditions => additional_conditions,
                                     :exclude => self.table.lock_opts)

      self.database.log("Done with subtable propagation from " +
               "#{self.parent.table_name} to #{self.table.table_name}")
      if self.table.subtable_filters.any?
        self.database.vacuum_and_reindex(self.table.table_name)
        self.table.subtable_filters.each(&:propagate_table!)
      end
    end

    def propagate!(old_parent_data, new_parent_data)
      return if (old_parent_data.empty? && new_parent_data.empty?)
      old_batch_keys = old_parent_data.map {|record| record[@opts[:primary_key]]}
      new_batch_keys = new_parent_data.map {|record| record[@opts[:primary_key]]}

      foreign_key = @opts[:foreign_key]
      finder_options = {foreign_key => old_batch_keys}
      if @opts[:type_key] && @opts[:type]
        finder_options[@opts[:type_key]] = @opts[:type]
      end

      old_records = table.get_records(finder_options)
      table.filter_batch(old_records) do |record|
        new_batch_keys.include?(record[foreign_key])
      end
    end

  end
end
