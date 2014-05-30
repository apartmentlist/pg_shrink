module PgShrink
  class Table
    attr_accessor :table_name
    attr_accessor :database
    attr_accessor :opts
    attr_reader :filters, :sanitizers, :subtable_filters, :subtable_sanitizers
    # TODO:  Figure out, do we need to be able to support tables with no
    # keys?  If so, how should we handle that?
    def initialize(database, table_name, opts = {})
      self.table_name = table_name
      self.database = database
      @opts = opts
      @filters = []
      @sanitizers = []
      @subtable_filters = []
      @subtable_sanitizers = []
    end

    def update_options(opts)
      @opts = @opts.merge(opts)
    end

    def filter_by(opts = {}, &block)
      self.filters << TableFilter.new(self, opts, &block)
    end

    def filter_subtable(table_name, opts = {})
      filter = SubTableFilter.new(self, table_name, opts)
      self.subtable_filters << filter
      yield filter.table if block_given?
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

    def sanitize_subtable(table_name, opts = {})
      sanitizer = SubTableSanitizer.new(self, table_name, opts)
      self.subtable_sanitizers << sanitizer
      yield sanitizer.table if block_given?
    end

    def update_records(original_records, new_records)
      if self.database
        database.update_records(self.table_name, original_records, new_records)
      end
    end

    def delete_records(old_records, new_records)
      if primary_key
        deleted_keys = old_records.map {|r| r[primary_key]} -
                       new_records.map {|r| r[primary_key]}
        if deleted_keys.any?
          self.database.delete_records(table_name, primary_key => deleted_keys)
        end
      else
        # TODO:  Do we need to speed this up?  Or is this an unusual enough
        # case that we can leave it slow?
        deleted_records = old_records - new_records
        deleted_records.each do |rec|
          self.database.delete_records(table_name, rec)
        end
      end
    end

    def records_in_batches(&block)
      if self.database
        self.database.records_in_batches(self.table_name, &block)
      else
        yield []
      end
    end

    def get_records(finder_options)
      if self.database
        self.database.get_records(self.table_name, finder_options)
      else
        []
      end
    end

    def filter_subtables(old_set, new_set)
      self.subtable_filters.each do |subtable_filter|
        subtable_filter.propagate!(old_set, new_set)
      end
    end

    def sanitize_subtables(old_set, new_set)
      self.subtable_sanitizers.each do |subtable_sanitizer|
        subtable_sanitizer.propagate!(old_set, new_set)
      end
    end

    def filter_batch(batch, &filter_block)
      new_set = batch.select do |record|
        locked?(record) || filter_block.call(record.dup)
      end
      delete_records(batch, new_set)
      filter_subtables(batch, new_set)
    end

    def sanitize_batch(batch, &sanitize_block)
      new_set = batch.map do |record|
        if locked?(record)
          record.dup
        else
          sanitize_block.call(record.dup)
        end
      end
      update_records(batch, new_set)
      sanitize_subtables(batch, new_set)
    end

    def filter!
      self.filters.each do |filter|
        self.records_in_batches do |batch|
          self.filter_batch(batch) do |record|
            filter.apply(record)
          end
        end
      end
    end

    def sanitize!
      self.sanitizers.each do |sanitizer|
        self.records_in_batches do |batch|
          self.sanitize_batch(batch) do |record|
            sanitizer.apply(record)
          end
        end
      end
    end

    # We use a filter for this, so that all other dependencies etc behave
    # as would be expected.
    def mark_for_removal!
      self.filter_by { false }
    end

    # Check explicitly for nil because we want to be able to set primary_key
    # to false for e.g. join tables
    def primary_key
      opts[:primary_key].nil? ? :id : opts[:primary_key]
    end

    def shrink!
      filter!
      sanitize!
    end
  end
end
