require 'spec_helper'

describe PgShrink::Table do
  context "when a filter is specified" do
    let(:database) {PgShrink::Database.new}
    let(:table) { PgShrink::Table.new(database, :test_table) }

    before(:each) do
      table.filter_by {|test| test[:u] == 1 }
    end

    it "should add filter to filters array" do
      expect(table.filters.size).to eq(1)
    end

    it "should accept values that match the block" do
      expect(table.filters.first.apply({:u => 1})).to eq(true)
    end

    it "should reject values that don't match the block" do
      expect(table.filters.first.apply({:u => 2})).to eq(false)
    end

    context "when running filters" do
      it "should return matching subset" do
        test_data = [{:u => 1}, {:u => 2}]
        expect(table).to receive(:records_in_batches).and_yield(test_data)
        expect(table).to receive(:delete_records) do |old_batch, new_batch|
          expect(old_batch.size).to eq(2)
          expect(new_batch.size).to eq(1)
          expect(new_batch.first).to eq({:u => 1})
        end
        table.filter!
      end
    end

    context "when locked" do
      before(:each) do
        table.lock { |test| !!test[:lock] }
      end

      it "should not filter locked records" do
        test_data = [{:u => 1, :lock => false},
                     {:u => 2, :lock => false},
                     {:u => 2, :lock => true}]
        allow(table).to receive(:records_in_batches).and_yield(test_data)
        allow(table).to receive(:delete_records) do |old_batch, new_batch|
          expect(old_batch.size).to eq(3)
          expect(new_batch.size).to eq(2)
          expect(new_batch).
            to eq([{:u => 1, :lock => false}, {:u => 2, :lock => true}])
        end
        table.filter!
      end
    end
  end

  context "when a sanitizer is specified" do
    let(:database) {PgShrink::Database.new}
    let(:table) { PgShrink::Table.new(database, :test_table) }
    before(:each) do
      table.sanitize do |test|
        test[:u] = -test[:u]
        test
      end
    end

    it "should add sanitizer to sanitizers array" do
      expect(table.sanitizers.size).to eq(1)
    end

    it "should alter values based on the block" do
      expect(table.sanitizers.first.apply({:u => 1})).to eq({:u => -1})
    end

    context "when running sanitizers" do
      it "returns an altered set of records" do
        test_data = [{:u => 1}, {:u => 2}]
        expect(table).to receive(:records_in_batches).and_yield(test_data)
        expect(table).to receive(:update_records) do |old_batch, new_batch|
          expect(old_batch).to eq(test_data)
          expect(new_batch).to eq([{:u => -1}, {:u => -2}])
        end
        table.sanitize!
      end
    end
  end

  context "when a subtable filter is specified" do
    let(:database) {PgShrink::Database.new}
    let(:table) { PgShrink::Table.new(database, :test_table, :primary_key => false) }

    before(:each) do
      table.filter_subtable(:subtable)
    end

    it "yields back a table so additional manipulations can be made" do
      table.filter_subtable(:subtable) do |f|
        expect(f.class).to eq(PgShrink::Table)
        expect(f.table_name).to eq(:subtable)
      end
    end

    it "adds subtable_filter to subtable_filters array" do
      expect(table.subtable_filters.size).to eq(1)
    end

    describe "when running filters" do
      before(:each) do
        table.filter_by do |test|
          !!test[:u]
        end
      end

      it "runs subtable filters with old and new batches" do
        test_data = [{:u => true}, {:u => false}]
        expect(table).to receive(:records_in_batches).and_yield(test_data)
        expect(database).to receive(:delete_records)
        expect(table).to receive(:filter_subtables) do |old_batch, new_batch|
          expect(old_batch).to eq(test_data)
          expect(new_batch).to eq([{:u => true}])
        end
        table.filter!
      end
    end
  end
  context "when a remove is specified" do
    let(:database) {PgShrink::Database.new}
    let(:table) { PgShrink::Table.new(database, :test_table) }
    let(:test_data) {[{:u => 1}, {:u => 2}]}

    before(:each) do
      table.mark_for_removal!
    end

    it "should run remove! if there are no dependencies" do
      expect(table).to receive(:remove!)
      table.shrink!
    end

    it "should allow locking of records" do
      table.lock do |u|
        u[:u] == 1
      end
      expect(table).to receive(:records_in_batches).and_yield(test_data)
      expect(table).to receive(:delete_records) do |old_batch, new_batch|
        expect(old_batch).to eq(test_data)
        expect(new_batch).to eq([{:u => 1}])
      end
      table.shrink!
    end

  end
end
