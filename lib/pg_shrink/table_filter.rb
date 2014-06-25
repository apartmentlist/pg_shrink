module PgShrink
  class TableFilter
    attr_accessor :table, :opts
    def initialize(table, opts = nil, &block)
      self.table = table
      @opts = opts
      @block = block if block_given?
    end
  end
end
