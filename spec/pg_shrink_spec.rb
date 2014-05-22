require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink do
  describe "simple two table filtering" do
    before(:all) do
      PgSpecHelper.create_database
      connection = PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres").connection
      PgSpecHelper.drop_table(connection, :users)
      PgSpecHelper.drop_table(connection, :user_preferences)
      PgSpecHelper.create_table(connection, :users, {'name' => 'character varying(256)', 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(connection, :user_preferences, {'user_id' => 'integer', 'name' => 'character varying(256)', 'value' => 'character varying(256)'})
    end

    let(:database) {PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres")}
    describe "with 20 users and associated preferences" do
      before(:each) do
        PgSpecHelper.clear_table(database.connection, :users)
        PgSpecHelper.clear_table(database.connection, :user_preferences)
        (1..20).each do |i|
          database.connection.run("insert into users (name, email) values ('test #{i}', 'test#{i}@test.com')")
          u = database.connection.from(:users).where(:name => "test #{i}").first
          (1..3).each do |j|
            database.connection.run("insert into user_preferences (user_id, name, value) values (#{u[:id]}, 'pref#{i}', 'prefvalue#{i}')")
          end
        end
      end

      describe "a simple filter and subtable" do
        before(:each) do
          database.filter_table(:users) do |f|
            f.filter_by do |u|
              u[:name] == "test 1"
            end
            f.filter_subtable(:user_preferences, :foreign_key => :user_id)
          end
          database.run_filters
        end
        it "will filter users down to the one matching" do
          remaining_users = database.connection.from(:users).all
          expect(remaining_users.size).to  eq(1)
        end
        it "will filter preferences to only those associated with the user" do
          remaining_user = database.connection.from(:users).first
          remaining_preferences = database.connection.from(:user_preferences).all
          expect(remaining_preferences.size).to eq(3)
          expect(remaining_preferences.map {|u| u[:user_id]}.uniq).to eq([remaining_user[:id]])
        end
      end
    end
  end
end
