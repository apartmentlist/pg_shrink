require 'spec_helper'

describe PgShrink::Database do
  it "should yield a table to filter_table" do
    db = PgShrink::Database.new
    db.filter_table(:test_table) do |tb|
      tb.is_a?(PgShrink::Table).should == true
      tb.database.should == db
      tb.table_name.should == :test_table
    end
  end

  it "should allow options to set a different primary_key in filter_table" do
    db = PgShrink::Database.new
    db.filter_table(:test_table, :primary_key => :foo) do |tb|
      tb.is_a?(PgShrink::Table).should == true
      tb.database.should == db
      tb.primary_key.should == :foo
    end
  end

  it "should retain options in later invocations" do
    db = PgShrink::Database.new
    db.filter_table(:test_table, :primary_key => :foo) do |tb|
      tb.is_a?(PgShrink::Table).should == true
      tb.database.should == db
      tb.primary_key.should == :foo
    end
    db.filter_table(:test_table) do |tb|
      tb.primary_key.should == :foo
    end
  end
end
