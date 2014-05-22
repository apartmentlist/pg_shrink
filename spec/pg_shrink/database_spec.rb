require 'spec_helper'

describe PgShrink::Database do
  let(:db) {PgShrink::Database.new}
  it "should yield a table to filter_table" do
    db.filter_table(:test_table) do |tb|
      expect(tb.class).to eq(PgShrink::Table)
      expect(tb.database).to eq(db)
      expect(tb.table_name).to eq(:test_table)
    end
  end

  it "should allow options to set a different primary_key in filter_table" do
    db.filter_table(:test_table, :primary_key => :foo) do |tb|
      expect(tb.primary_key).to eq(:foo)
    end
  end

  it "should retain options in later invocations" do
    db.filter_table(:test_table, :primary_key => :foo) {}
    db.filter_table(:test_table) do |tb|
      expect(tb.primary_key).to eq(:foo)
    end
  end
end
