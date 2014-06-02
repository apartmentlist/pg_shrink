module PgShrink
  class TableFilter
    attr_accessor :table, :opts
    def initialize(table, opts = nil, &block)
      self.table = table
      # TODO: Figure out how to deal with opts and block
      @opts = opts
      @block = block if block_given?
    end

    def conditions?
      @opts && @opts.any? && !@block
    end

    def apply(hash)
      @block.call(hash)
    end
  end
end
