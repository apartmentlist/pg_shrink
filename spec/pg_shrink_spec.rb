require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink do
  before(:all) do
    PgSpecHelper.reset_database
  end

  let(:database) {PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres")}

  describe "simple foreign_key setup" do
    before(:all) do
      # Rspec doesn't want you using 'let' defined things in before(;all)
      connection = PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres").connection
      PgSpecHelper.create_table(connection, :users,
                                {'name' => 'character varying(256)',
                                 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(connection, :user_preferences,
                                {'user_id' => 'integer',
                                 'name' => 'character varying(256)',
                                 'value' => 'character varying(256)'})
    end

    describe "simple two table filtering" do
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
            database.filter!
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

        describe "a simple filter and subtable with sanitization on each" do
          before(:each) do
            database.filter_table(:users) do |f|
              f.filter_by do |u|
                u[:name] == "test 1"
              end
              f.sanitize do |u|
                u[:name] = "sanitized #{u[:name]}"
                u[:email] = "blank_email#{u[:id]}@foo.bar"
                u
              end
              f.filter_subtable(:user_preferences, :foreign_key => :user_id)
            end
            database.filter_table(:user_preferences) do |f|
              f.sanitize do |u|
                u[:value] = "sanitized #{u[:value]}"
                u
              end
            end
            database.shrink!
          end

          it "should result in 1 sanitized users" do
            remaining_users = database.connection.from(:users).all
            expect(remaining_users.size).to  eq(1)
            expect(remaining_users.first[:name]).to match(/sanitized/)
            expect(remaining_users.first[:email]).to match(/blank_email/)
          end

          it "should result in 3 sanitized preferences" do
            remaining_user = database.connection.from(:users).first
            remaining_preferences = database.connection.from(:user_preferences).all
            expect(remaining_preferences.size).to eq(3)
            expect(remaining_preferences.map {|u| u[:value]}.grep(/sanitized/).size).to eq(3)
          end
        end
      end
    end


    describe "three table filter chain" do
      before(:all) do
        # Rspec doesn't want you using 'let' defined things in before(;all)
        connection = PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres").connection
        PgSpecHelper.create_table(connection, :user_preference_values,
                                  {'user_preference_id' => 'integer', 'value' =>
                                   'character varying(256)'})
      end

      describe "with 20 users and associated preferences" do
        before(:each) do
          PgSpecHelper.clear_table(database.connection, :users)
          PgSpecHelper.clear_table(database.connection, :user_preferences)
          PgSpecHelper.clear_table(database.connection, :user_preference_values)
          (1..20).each do |i|
            database.connection.run("insert into users (name, email) values ('test #{i}', 'test#{i}@test.com')")
            u = database.connection.from(:users).where(:name => "test #{i}").first
            (1..3).each do |j|
              database.connection.
                run("insert into user_preferences (user_id, name) values (#{u[:id]}, 'pref#{i}#{j}')")
              pref = database.connection.from(:user_preferences).where(:name => "pref#{i}#{j}").first
              database.connection.
                run("insert into user_preference_values(user_preference_id, value) " +
                    "values (#{pref[:id]}, 'val#{i}#{j}')")
            end
          end
        end

        describe "a simple filter and chained subtables" do
          before(:each) do
            database.filter_table(:users) do |f|
              f.filter_by do |u|
                u[:name] == "test 1"
              end
              f.filter_subtable(:user_preferences, :foreign_key => :user_id)
            end
            database.filter_table(:user_preferences) do |f|
              f.filter_subtable(:user_preference_values, :foreign_key => :user_preference_id)
            end

            database.filter!
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
          it "will filter preference values to only those associated with the preferences remaining" do
            remaining_user = database.connection.from(:users).first
            remaining_preferences = database.connection.from(:user_preferences).all
            remaining_preference_values = database.connection.from(:user_preference_values).all
            expect(remaining_preference_values.size).to eq(3)
            expect(remaining_preference_values.map {|v| v[:user_preference_id]}).to(
              match_array(remaining_preferences.map {|p| p[:id]})
            )
          end
        end
      end
    end
  end
  describe "polymorphic foreign key subtables" do
    before(:all) do
      # Rspec doesn't want you using 'let' defined things in before(;all)
      connection = PgShrink::Database::Postgres.new(:database => 'test_pg_shrink', :user => "postgres").connection
      PgSpecHelper.create_table(connection, :users,
                                {'name' => 'character varying(256)',
                                 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(connection, :preferences,
                                {'context_id' => 'integer',
                                 'context_type' => 'character varying(256)',
                                 'name' => 'character varying(256)',
                                 'value' => 'character varying(256)'})
    end
    describe "with 20 users, associated preferences, and some extra preferences for a different type" do
      before(:each) do
        PgSpecHelper.clear_table(database.connection, :users)
        PgSpecHelper.clear_table(database.connection, :preferences)
        (1..20).each do |i|
          database.connection.run("insert into users (name, email) values ('test #{i}', 'test#{i}@test.com')")
          u = database.connection.from(:users).where(:name => "test #{i}").first
          (1..3).each do |j|
            database.connection.run("insert into preferences (context_id, context_type, name, value) values (#{u[:id]}, 'User', 'pref#{i}', 'prefvalue#{i}')")
          end
          database.connection.run("insert into preferences (context_id, context_type, name, value) values(#{u[:id]}, 'OtherClass', 'pref#{i}', 'prefvalue#{i}')")
        end
      end

      describe "simple two table filtering" do
        before(:each) do
          database.filter_table(:users) do |f|
            f.filter_by do |u|
              u[:name] == "test 1"
            end
            f.filter_subtable(:preferences, :foreign_key => :context_id, :type_key => :context_type, :type => 'User')
          end
          database.filter!
        end

        it "will filter preferences with context_type 'User' to only those associated with the user" do
          remaining_user = database.connection.from(:users).first
          remaining_preferences = database.connection.from(:preferences).where(:context_type => 'User').all
          expect(remaining_preferences.size).to eq(3)
          expect(remaining_preferences.map {|u| u[:context_id]}.uniq).to eq([remaining_user[:id]])
        end

        it "will not filter preferences without context_type user" do
          remaining_preferences = database.connection.from(:preferences).where(:context_type => 'OtherClass').all
          expect(remaining_preferences.size).to eq(20)
        end
      end
    end
  end
end
