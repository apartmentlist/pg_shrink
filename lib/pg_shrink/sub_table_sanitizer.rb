module PgShrink
  class SubTableSanitizer < SubTableOperator

    def validate_opts!(opts)
      unless opts[:local_field] && opts[:foreign_field]
        raise "Error: #{name} must define :local_field and :foreign_field"
      end
      super(opts)
    end

    def propagate!(old_parent_data, new_parent_data)
      return if (old_parent_data.empty? && new_parent_data.empty?)
      old_batch = old_parent_data.index_by {|record| record[@opts[:primary_key]]}
      new_batch = new_parent_data.index_by {|record| record[@opts[:primary_key]]}

      foreign_key = @opts[:foreign_key]
      finder_options = {foreign_key => old_batch.keys}
      if @opts[:type_key] && @opts[:type]
        finder_options[@opts[:type_key]] = @opts[:type]
      end

      parent_field = @opts[:local_field].to_sym
      child_field = @opts[:foreign_field].to_sym

      old_child_records = table.get_records(finder_options)
      table.sanitize_batch(old_child_records) do |record|
        parent_record = new_batch[record[foreign_key]]
        record[child_field] = parent_record[parent_field]
        record
      end
    end

  end
end
