module PgShrink
  class SubTable
    attr_accessor :parent, :table_name, :database

    def default_opts
      {:foreign_key =>
        "#{ActiveSupport::Inflector.singularize(parent.table_name.to_s)}_id",
       :primary_key => :id
      }
    end

    def initialize(parent, table_name, opts = {})
      self.parent = parent
      self.table_name = table_name
      self.database = parent.database
      @opts = default_opts.merge(opts)
      if @opts[:type_key] && !@opts[:type]
        raise "Error:  #{table_name} subtable has type_key set but no type"
      end
      if @opts[:type] && !@opts[:type_key]
        raise "Error:  #{table_name} subtable has type set but no type_key"
      end
    end

    def table
      database.table(self.table_name)
    end

    def propagate_filters(old_parent_data, new_parent_data)
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
