module PgShrink
  class Table
    attr_accessor :table_name
    def initialize(table_name)
      self.table_name = table_name
    end

    def filters
      @filters ||= []
    end

    def sanitizers
      @sanitizers ||= []
    end

    def filter_by(opts = {}, &block)
      self.filters << TableFilter.new(self, opts, &block)
    end

    def lock(opts = {}, &block)
      @lock = block
    end

    def sanitize(opts = {}, &block)
      self.sanitizers << TableSanitizer.new(self, opts, &block)
    end

    def filter_subtable(subtable, opts = {})
      # TODO:  Do something
    end
  end
end
