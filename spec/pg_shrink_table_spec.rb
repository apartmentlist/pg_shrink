require 'spec_helper'

describe PgShrink::Table do
  before(:each) do
    @table = PgShrink::Table.new(:test_table)
  end

  it "should set up filters and add them to an array" do
    @table.filter_by do |test|
      !!test[:u]
    end
    @table.filters.size.should == 1
    @table.filters.first.is_a?(PgShrink::TableFilter).should == true
  end

  it "should properly pass along the block to table filters" do
    @table.filter_by do |test|
      test[:u] == 1
    end
    @table.filters.first.apply({:u => 1}).should == true
    @table.filters.first.apply({:u => 2}).should == false
  end

  it "should set up sanitizers and add them to an array" do
    @table.sanitize do |test|
      test[:u] = "foo"
      test
    end
    @table.sanitizers.size.should == 1
    @table.sanitizers.first.is_a?(PgShrink::TableSanitizer).should == true
  end

  it "should properly pass along the block to table sanitizers" do
    @table.sanitize do |test|
      test[:u] = 1
      test
    end
    @table.sanitizers.first.apply({:u => 0}).should == {:u => 1}
  end

  it "Should be able to run filters and return a subset of records" do
    @table.filter_by do |test|
      !!test[:u]
    end
    test_data = [{:u => true}, {:u => false}]
    allow(@table).to receive(:records_in_batches).and_return([test_data])
    allow(@table).to receive(:update_records) do |*args|
      args.size.should == 2
      old_batch = args.first
      new_batch = args.last
      old_batch.size.should == 2
      new_batch.size.should == 1
      new_batch.first.should == {:u => true}
    end
    @table.run_filters
  end

  it "Should be able to run sanitization and return an altered set of records" do
    @table.sanitize do |test|
      test[:u] = -test[:u]
      test
    end
    test_data = [{:u => 1}, {:u => 2}]
    allow(@table).to receive(:records_in_batches).and_return([test_data])
    allow(@table).to receive(:update_records) do |*args|
      args.size.should == 2
      old_batch = args.first
      new_batch = args.last
      old_batch.size.should == 2
      new_batch.size.should == 2
      old_batch.should == [{:u => 1}, {:u => 2}]
      new_batch.should == [{:u => -1}, {:u => -2}]
    end
    @table.run_sanitizers
  end
end
