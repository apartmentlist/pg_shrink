module PgShrink
  class Table
    attr_accessor :table_name
    attr_accessor :database
    attr_accessor :opts
    attr_reader :filters, :sanitizers, :subtable_filters, :subtable_sanitizers, :lock_opts
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
      @lock_opts = opts
      if block_given?
        puts "WARNING:  Block-based lock on #{self.table_name} will make things SLOW"
        @lock_block = block
      end
    end

    def has_lock?
      (@lock_opts && @lock_opts.any?) || @lock_block
    end

    def lock_condition_ok?
      !@lock_block
    end

    def locked?(record)
      if @lock_block
        @lock_block.call(record)
      elsif @lock_opts && @lock_opts.any?
        raise "unimplemented"
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

    def condition_filter(filter)
      self.database.delete_records(self.table_name, {}, [filter.opts, @lock_opts].compact)
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
        sanitize_block.call(record.dup)
      end
      update_records(batch, new_set)
      sanitize_subtables(batch, new_set)
    end

    def filter!
      if remove? && can_just_remove?
        remove!
      else
        self.filters.each do |filter|
          if filter.conditions? && self.lock_condition_ok?
            self.condition_filter(filter)
            self.subtable_filters.each(&:propagate_table!)
          else
            self.records_in_batches do |batch|
              self.filter_batch(batch) do |record|
                filter.apply(record)
              end
            end
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

    def can_just_remove?
      self.subtable_filters.empty? && self.subtable_sanitizers.empty? && !has_lock?
    end

    # Mark @remove and add filter so that if we're in the simple case we can
    # just remove! and if not we can just go through filters and all
    # dependencies will be handled
    def mark_for_removal!
      @remove = true
      self.filter_by { false }
    end

    def remove?
      !!@remove
    end

    def remove!
      self.database.delete_records(table_name, {})
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
