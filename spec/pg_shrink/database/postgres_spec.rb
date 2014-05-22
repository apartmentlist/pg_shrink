require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink::Database::Postgres do
  before(:all) do
    PgSpecHelper.create_database
    PgSpecHelper.drop_table(:test_table)
    PgSpecHelper.create_table(:test_table, {'name' => 'character(128)', 'test' => 'integer'})
  end
  before(:each) do
    PgSpecHelper.clear_table(:test_table)
  end

  it "should be creatable and set up a Sequel connection" do
    db =PgShrink::Database::Postgres.new(:database => 'test_pg_shrink')
    db.connection.is_a?(Sequel::Postgres::Database).should == true
  end

  it "should be able to fetch records in batches" do
    db =PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :batch_size => 5, :user => 'postgres')
    (1..20).each do |i|
      db.connection.run("insert into test_table (name, test) values ('test', #{i})")
    end
    batches = []
    batch_sizes = []
    db.records_in_batches(:test_table) do |batch|
      batch_sizes << batch.size
    end
    batch_sizes.should == [5, 5, 5, 5]
  end

  it "should be able to delete records based on a condition" do
    db =PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => 'postgres')
    (1..20).each do |i|
      db.connection.run("insert into test_table (name, test) values ('test', #{i})")
    end

    db.delete_records(:test_table, {:test => 1..5})

    results = db.connection["select * from test_table"].all
    results.size.should == 15
    results.map {|r| r[:test]}.sort.should == (6..20).to_a.sort
  end

  it "should be able to update records" do
    db =PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => 'postgres')
    (1..20).each do |i|
      db.connection.run("insert into test_table (name, test) values ('test', #{i})")
    end

    old_records = db.connection["select * from test_table where test <= 5"].all
    new_records = old_records.map {|r| r.merge(:test => r[:test] * 10)}
    db.update_records('test_table', old_records, new_records)
    db.connection["select * from test_table where test <= 5"].all.size.should == 0
    updated_records = db.connection[:test_table].where(:id => old_records.map {|r| r[:id]}).all
    updated_records.size.should == old_records.size
    updated_records.should == new_records
  end

  it "should be able to handle deletions in update records" do
    db =PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => 'postgres')
    (1..20).each do |i|
      db.connection.run("insert into test_table (name, test) values ('test', #{i})")
    end

    old_records = db.connection["select * from test_table where test <= 5"].all
    new_records = old_records[0..1]
    db.update_records('test_table', old_records, new_records)
    db.connection["select * from test_table where test <= 5"].all.size.should == 2
    updated_records = db.connection[:test_table].where(:id => old_records.map {|r| r[:id]}).all
    updated_records.size.should == new_records.size
    updated_records.should == new_records
  end
end
