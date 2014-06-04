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
      # use !empty instead of any? because we accept string conditions
      !@block
    end

    def apply(hash)
      if @block
        @block.call(hash)
      # if we have a straightforwards conditions hash can just do in place comparisons
      elsif @opts.is_a?(Hash)
        @opts.each do |k, v|
          if [Array, Range].include?(v.class)
            return false unless v.include?(hash[k])
          elsif [String, Integer, Float].include?(v.class)
            return false unless hash[k] == v
          else
            raise "Unsupported condition type for mixing with block locks: #{v.class}"
          end
        end
        return true
      #TODO:  Figure out if this case matters and we want to support it.
      elsif @opts.is_a?(String)
        raise "Unsupported:  Mixing string conditions with block locks"
      end
    end
  end
end
