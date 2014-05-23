module PgShrink
  class SubTable
    attr_accessor :parent, :table_name, :database

    def default_opts
      {:foreign_key => "#{ActiveSupport::Inflector.singularize(parent.table_name.to_s)}_id",
       :primary_key => :id
      }
    end

    def initialize(parent, table_name, opts = {})
      self.parent = parent
      self.table_name = table_name
      self.database = parent.database
      @opts = self.default_opts.merge(opts)
    end

    def table
      self.database.table(self.table_name)
    end

    # TODO:  This kind of feels like it should actually use a TableFilter,
    # but it's different because it relies on outside input.  Perhaps a
    # TableFilter should be able to take additional arguments?  Or maybe this
    # is good enough.
    def propagate_filters(old_parent_data, new_parent_data)
      old_batch_keys = old_parent_data.map {|record| record[@opts[:primary_key]]}
      new_batch_keys = new_parent_data.map {|record| record[@opts[:primary_key]]}

      foreign_key = @opts[:foreign_key]
      old_records = table.get_records(foreign_key => old_batch_keys)
      table.filter_batch(old_records) do |record|
        new_batch_keys.include?(record[foreign_key])
      end
    end

  end
end
