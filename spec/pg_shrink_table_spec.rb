require 'spec_helper'

describe PgShrink::Table do
  context "when a filter is specified" do
    let(:table) { PgShrink::Table.new(:test_table) }
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
        expect(table).to receive(:records_in_batches).and_return([test_data])
        expect(table).to receive(:update_records) do |*args|
          args.size.should == 2
          old_batch = args.first
          new_batch = args.last
          old_batch.size.should == 2
          new_batch.size.should == 1
          new_batch.first.should == {:u => 1}
        end
        table.run_filters
      end
    end
    context "when locked" do
      it "should not filter locked records" do
        table.lock do |test|
          !!test[:lock]
        end
        test_data = [{:u => 1, :lock => false}, {:u => 2, :lock => false}, {:u => 2, :lock => true}]
        allow(table).to receive(:records_in_batches).and_return([test_data])
        allow(table).to receive(:update_records) do |*args|
          args.size.should == 2
          old_batch = args.first
          new_batch = args.last
          old_batch.size.should == 3
          new_batch.size.should == 2
          new_batch.should == [{:u => 1, :lock => false}, {:u => 2, :lock => true}]
        end
        table.run_filters
      end
    end
  end
  
  context "when a sanitizer is specified" do
    before(:each) do 
      table.sanitize do |test|
        test[:u] = -test[:u]
        test
      end
      it "should add sanitizer to sanitizers array" do
        expect(table.sanitizers.size).to eq(1)
      end
      it "should alter values based on the block" do
        expect(table.filters.first.apply({:u => 1})).to eq({:u => -1})
      end
      context "when running sanitizers" do
        it "returns an altered set of records" do
          test_data = [{:u => 1}, {:u => 2}]
          expect(table).to receive(:records_in_batches).and_return([test_data])
          expect(table).to receive(:update_records) do |*args|
            args.size.should == 2
            old_batch = args.first
            new_batch = args.last
            old_batch.size.should == 2
            new_batch.size.should == 2
            old_batch.should == [{:u => 1}, {:u => 2}]
            new_batch.should == [{:u => -1}, {:u => -2}]
          end
          table.run_sanitizers
        end
      end
    end
  end

  it "Should be able to add a subtable filter" do
    @table.filter_subtable(:subtable)
    @table.subtables.size.should == 1
    @table.subtables.first.is_a?(PgShrink::SubTable).should == true
  end

  it "Should run subtable filters with old and new batches when running filters" do
    @table.filter_by do |test|
      !!test[:u]
    end
    @table.filter_subtable(:subtable)
    test_data = [{:u => true}, {:u => false}]
    allow(@table).to receive(:records_in_batches).and_return([test_data])
    expect(@table).to receive(:filter_subtables) do |*args|
      args.size.should == 2
      old_batch = args.first
      new_batch = args.last
      old_batch.should == [{:u => true}, {:u => false}]
      new_batch.should == [{:u => true}]
    end
    @table.run_filters
  end
end
