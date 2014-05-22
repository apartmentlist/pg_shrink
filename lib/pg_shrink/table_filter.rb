module PgShrink
  class TableFilter
    attr_accessor :table
    def initialize(table, opts, &block)
      self.table = table
      @opts = opts # Currently not used, but who knows
      @block = block
    end

    def apply(hash)
      @block.call(hash)
    end
  end
end
