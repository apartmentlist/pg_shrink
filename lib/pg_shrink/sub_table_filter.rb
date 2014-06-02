module PgShrink
  class SubTableFilter < SubTableOperator

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
