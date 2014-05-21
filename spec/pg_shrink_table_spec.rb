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
end
