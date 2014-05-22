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
end
