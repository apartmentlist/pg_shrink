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

    def locked?(record)
      if @lock
        @lock.call(record)
      end
    end

    def sanitize(opts = {}, &block)
      self.sanitizers << TableSanitizer.new(self, opts, &block)
    end

    # TODO: This is a little awkward... need to figure out the right way to do
    # this, but the core idea is that because the set of records is likely to
    # be too large to load all at once, we want to load it in batches, and
    # after each batch commit back changes.  So the data_source should define both
    # an update_records method and a records_in_batches method.
    #
    # The update_records method then takes a set of original records and a new
    # set of records.  It deletes any records that were in the original set but
    # not the new set, and does any updates necessary between the new and old
    # set.
    #
    # records_in_batches should be enumerable and on each time through yield a set
    # of records.
    #
    #  TODO:  Figure out if we need to distinguish between filters and
    #  sanitizers at this level?  IE does the callback need to enforce the
    #  difference between filtering and updating?
    def update_records(original_records, new_records)
      if self.data_source
        data_source.update_records(original_records, new_records)
      end
    end

    def records_in_batches
      if self.data_source
        self.data_source.records_in_batches
      else
        [[]]
      end
    end

    def run_filters
      self.filters.each do |filter|
        self.records_in_batches.each do |batch|
          new_set = batch.select do |record|
            self.locked?(record) || filter.apply(record.dup)
          end
          self.update_records(batch, new_set)
          # TODO:  Trickle down any filtering dependencies to subtables.
        end
      end
    end

    def run_sanitizers
      self.sanitizers.each do |filter|
        self.records_in_batches.each do |batch|
          new_set = batch.map {|record| filter.apply(record.dup)}
          self.update_records(batch, new_set)
          # TODO:  Trickle down any sanitization dependencies to subtables.
        end
      end
    end

    def run
      run_filters
      run_sanitizers
    end
  end
end
