module PgShrink
  class Table
    attr_accessor :table_name
    attr_accessor :data_source
    def initialize(table_name, data_source = nil)
      self.table_name = table_name
      self.data_source = data_source
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

    # TODO:  This is a little awkward... need to figure out the right way to do this, but the core idea is
    # that because the set of records is likely to be too large to load all at once, we want to load
    # it in batches, and after each batch commit back changes.  So records_in_batches should return a
    # tuple of [records, callback_function]
    # where callback_function responds to 'call' (ie is a lambda or proc) and accepts the new set of
    # records.  It should then delete any records from the original batch of records that are not
    # in the new set.
    #
    #  TODO:  Do we need to distinguish between filters and sanitizers at this level?  IE does the
    #  callback need to enforce the difference between filtering and updating?
    def records_in_batches
      if self.data_source
        self.data_source.records_in_batches
      else
        [[], lambda {|set|}]
      end
    end

    def run_filters
      self.filters.each do |filter|
        self.records_in_batches.each do |batch, update_fn|
          new_set = batch.select {|record| filter.apply(record)}
          update_fn.call(new_set)
        end
      end
    end

    def run_sanitizers
    end

    def run
      run_filters
      run_sanitizers
    end
  end
end
