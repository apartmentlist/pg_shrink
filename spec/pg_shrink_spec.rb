require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink do
  describe "simple two table filtering" do
    before(:all) do
      PgSpecHelper.create_database
      PgSpecHelper.drop_table(:users)
      PgSpecHelper.drop_table(:user_preferences)
      PgSpecHelper.create_table(:users, {'name' => 'character varying(256)', 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(:user_preferences, {'user_id' => 'integer', 'name' => 'character varying(256)', 'value' => 'character varying(256)'})
    end

    before(:each) do
      PgSpecHelper.clear_table(:users)
      PgSpecHelper.clear_table(:user_preferences)
      @database = PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres")
      (1..20).each do |i|
        @database.connection.run("insert into users (name, email) values ('test #{i}', 'test#{i}@test.com')")
        u = @database.connection.from(:users).where(:name => "test #{i}").first
        (1..3).each do |j|
          @database.connection.run("insert into user_preferences (user_id, name, value) values (#{u[:id]}, 'pref#{i}', 'prefvalue#{i}')")
        end
      end
    end

    it "Should allow simple cascading filters" do
      @database.filter_table(:users) do |f|
        f.filter_by do |u|
          u[:name] == "test 1"
        end
        f.filter_subtable(:user_preferences, :foreign_key => :user_id)
      end
      @database.run_filters

      remaining_users = @database.connection.from(:users).all
      remaining_users.size.should == 1
      remaining_preferences = @database.connection.from(:user_preferences).all
      remaining_preferences.size.should == 3
      remaining_preferences.map {|u| u[:user_id]}.uniq.should == [remaining_users.first[:id]]
    end
  end
end
