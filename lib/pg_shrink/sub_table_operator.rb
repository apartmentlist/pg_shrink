module PgShrink
  class SubTableOperator
    attr_accessor :parent, :table_name, :database
    def default_opts
      {:foreign_key =>
        "#{ActiveSupport::Inflector.singularize(parent.table_name.to_s)}_id",
       :primary_key => :id
      }
    end

    def name
      "#{table_name} #{self.class.name.demodulize}"
    end

    def table
      database.table(table_name)
    end

    def validate_opts(opts)
      if opts[:type_key] && !opts[:type]
        raise "Error:  #{name} has type_key set but no type"
      end
      if opts[:type] && !opts[:type_key]
        raise "Error:  #{name} has type set but no type_key"
      end
    end

    def initialize(parent, table_name, opts = {})
      self.parent = parent
      self.table_name = table_name
      self.database = parent.database
      @opts = default_opts.merge(opts)

      validate_opts(@opts)
    end

    def propagate!(old_parent_data, new_parent_data)
      raise "Implement in subclass"
    end


  end
end

