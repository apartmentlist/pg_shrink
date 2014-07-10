require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink::Database::Postgres do
  let(:db) do
    PgShrink::Database::Postgres.new(PgSpecHelper.pg_config.merge(:batch_size => 5))
  end

  before(:all) do
    PgSpecHelper.reset_database

    # id column created implicitly
    PgSpecHelper.create_table(db.connection, :test_table,
                              {'name' => 'character(128)', 'value' => 'integer'})

    # id column created implicitly
    PgSpecHelper.create_table(db.connection, :test_sub_table,
                              {'test_table_id' => 'integer', 'value' => 'character(128)', })
  end

  before(:each) do
    PgSpecHelper.clear_table(db.connection, :test_table)
    PgSpecHelper.clear_table(db.connection, :test_sub_table)
  end

  context 'A simple postgres database' do
    it 'sets up a Sequel connection' do
      expect(db.connection.class).to eq(Sequel::Postgres::Database)
    end

    context 'with 20 simple records' do
      before(:each) do
        (1..20).each do |i|
          db.connection.run(
            "insert into test_table (name, value) values ('test', #{i})"
          )
        end
      end

      it 'can fetch records in batches' do
        batch_sizes = []
        db.records_in_batches(:test_table) do |batch|
          batch_sizes << batch.size
        end
        expect(batch_sizes).to eq([5, 5, 5, 5])
      end

      it 'throws an error if records change their primary keys during update' do
        old_records = db.connection['select * from test_table where value <= 5'].
          all
        new_records = old_records.map {|r| r.merge(:id => r[:id] * 10)}
        expect {db.update_records(:test_table, old_records, new_records)}.
          to raise_error
      end

      it 'can delete records based on a condition' do
        db.delete_records(:test_table, {:value => 1..5})

        results = db.connection['select * from test_table'].all
        expect(results.size).to eq(15)
        expect(results.map {|r| r[:value]}).to match_array((6..20).to_a)
      end

      it 'can update records' do
        old_records = db.connection['select * from test_table where value <= 5'].
          all
        new_records = old_records.map {|r| r.merge(:value => r[:value] * 10)}
        db.update_records(:test_table, old_records, new_records)
        expect(
          db.connection['select * from test_table where value <= 5'].all.size
        ).to eq(0)
        updated_records = db.connection[:test_table].
          where(:id => old_records.map {|r| r[:id]}).all
        expect(updated_records.size).to eq(old_records.size)
        expect(updated_records).to eq(new_records)
      end

      it 'throws an error if you try to delete records in update' do
        old_records = db.connection['select * from test_table where value <= 5'].
          all
        new_records = old_records.first(2)
        expect {db.update_records(:test_table, old_records, new_records)}.
          to raise_error
      end

      it 'deletes the whole table' do
        db.remove_table(:test_table)
        db.filter!
        expect(db.connection['select * from test_table'].all.size).to eq(0)
      end


      describe 'on a table with no primary key' do

        before(:all) do
          PgSpecHelper.create_table(db.connection, :no_primary_key,
                              {'name' => 'character(128)',
                               'test' => 'integer'}, nil)
        end

        before(:each) do
          PgSpecHelper.clear_table(db.connection, :no_primary_key)
          (1..20).each do |i|
            db.connection.run(
              "insert into no_primary_key (name, test) values ('test', #{i})")
          end
        end

        it 'can still remove the whole table' do
          db.remove_table(:no_primary_key, :primary_key => false)
          db.filter!
          expect(db.connection['select * from no_primary_key'].all.size).to eq(0)

        end

      end

    end
  end

  context 'A table and subtable' do

    before(:each) do
      table_values = [
        [1, 'john'],
        [2, 'chris'],
        [3, 'matt'],
      ]

      sub_table_values = [
        [1, 'john_value_1'],
        [1, 'john_value_2'],

        [2, 'chris_value_1'],

        [3, 'matt_value_1'],
        [3, 'matt_value_2'],
        [3, 'matt_value_3'],
      ]

      table_values.each do |row|
        db.connection.run(
          "insert into test_table (id, name, value) values (#{row[0]}, '#{row[1]}', #{row[0]})")
      end

      sub_table_values.each do |row|
        db.connection.run(
          "insert into test_sub_table (test_table_id, value) values (#{row[0]}, '#{row[1]}')")
      end
    end

    context 'delete' do

      before(:each) do
        db.connection.run(
          "delete from test_table where name = 'matt'"
        )
      end

      it 'propagates to subtable' do

        db.propagate_delete(
          parent_table: 'test_table',
          child_table: 'test_sub_table',
          parent_key: 'id',
          child_key: 'test_table_id')

        expect(db.connection['select * from test_sub_table where test_table_id = 3'].all.size).to eq(0)
      end

      it 'propagates to subtable honoring where restriction' do
        db.propagate_delete(
          parent_table: 'test_table',
          child_table: 'test_sub_table',
          parent_key: 'id',
          child_key: 'test_table_id',
          where: "value = 'matt_value_2'")

        expect(db.connection['select * from test_sub_table where test_table_id = 3'].all.size).to eq(2)
      end

      it 'propagates to subtable (nil where ignored)' do
        db.propagate_delete(
          parent_table: 'test_table',
          child_table: 'test_sub_table',
          parent_key: 'id',
          child_key: 'test_table_id',
          where: nil)

        expect(db.connection['select * from test_sub_table where test_table_id = 3'].all.size).to eq(0)
      end

    end
  end
end
