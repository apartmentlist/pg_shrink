module PgShrink
  class Table
    attr_accessor :table_name
    attr_accessor :database
    attr_accessor :opts
    attr_reader :filters, :sanitizers, :subtables
    # TODO:  Figure out, do we need to be able to support tables with no
    # keys?  If so, how should we handle that?
    def initialize(database, table_name, opts = {})
      self.table_name = table_name
      self.database = database
      @opts = opts
      @filters = []
      @sanitizers = []
      @subtables = []
    end

    def update_options(opts)
      @opts = @opts.merge(opts)
    end

    def filter_by(opts = {}, &block)
      self.filters << TableFilter.new(self, opts, &block)
    end

    def filter_subtable(table_name, opts = {})
      self.subtables << SubTable.new(self, table_name, opts)
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

    #  TODO:  Figure out if we need to distinguish between filters and
    #  sanitizers at this level?  IE does the callback need to enforce the
    #  difference between filtering and updating?
    #
    #  Or is it good enough that the database handles all of this?
    def update_records(original_records, new_records)
      if self.database
        database.update_records(self.table_name, original_records, new_records)
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
      self.subtables.each do |subtable|
        subtable.propagate_filters(old_set, new_set)
      end
    end

    def filter_batch(batch, &filter_block)
      new_set = batch.select do |record|
        self.locked?(record) || filter_block.call(record.dup)
      end
      self.update_records(batch, new_set)
      self.filter_subtables(batch, new_set)
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
      self.sanitizers.each do |filter|
        self.records_in_batches do |batch|
          new_set = batch.map {|record| filter.apply(record.dup)}
          self.update_records(batch, new_set)
          # TODO:  Trickle down any sanitization dependencies to subtables.
        end
      end
    end

    def primary_key
      self.opts[:primary_key] || :id
    end

    def run
      filter!
      sanitize!
    end
  end
end
