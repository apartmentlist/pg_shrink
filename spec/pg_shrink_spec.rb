require 'spec_helper'
require 'pg_spec_helper'

describe PgShrink do
  before(:all) do
    PgSpecHelper.reset_database
  end

  let(:database) {
    PgShrink::Database::Postgres.new(PgSpecHelper.pg_config)
  }
  after(:each) do
    database.connection.disconnect
  end

  describe "simple foreign_key setup" do
    before(:all) do
      # Rspec doesn't want you using 'let' defined things in before(:all)
      connection = PgShrink::Database::Postgres.new(PgSpecHelper.pg_config).
                   connection
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
            database.connection.run("insert into users (name, email) " +
                                    "values ('test #{i}', 'test#{i}@test.com')")
            u = database.connection.from(:users).where(:name => "test #{i}").first
            (1..3).each do |j|
              database.connection.run(
                "insert into user_preferences (user_id, name, value) " +
                "values (#{u[:id]}, 'pref#{i}', 'prefvalue#{i}')"
              )
            end
          end
        end

        describe "with a test shrinkfile" do
          let(:shrinkfile) {"spec/Shrinkfile.basic"}
          let(:url) {database.connection_string}

          it "should set up a postgres database" do
            expect(PgShrink::Database::Postgres).to receive(:new) do |opts|
                expect(opts[:postgres_url]).to eq(database.connection_string)
              end.and_return(database)
            PgShrink.run(config: shrinkfile, url: url, force: true)
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
            expect(remaining_users.size).to eq(1)
          end

          it "will filter preferences to only those associated with the user" do
            remaining_user = database.connection.from(:users).first
            remaining_preferences = database.connection.
              from(:user_preferences).all
            expect(remaining_preferences.size).to eq(3)
            expect(remaining_preferences.map {|u| u[:user_id]}.uniq).
              to eq([remaining_user[:id]])
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
            remaining_preferences = database.connection.
              from(:user_preferences).all
            expect(remaining_preferences.size).to eq(3)
            expect(remaining_preferences.all? do |p|
              p[:value] =~ /sanitized/
            end).to be_true
          end
        end
      end
      describe "with users and preferences including email as value" do
        before(:each) do
          PgSpecHelper.clear_table(database.connection, :users)
          PgSpecHelper.clear_table(database.connection, :user_preferences)
          (1..20).each do |i|
            database.connection.run("insert into users (name, email) " +
                                    "values ('test #{i}', 'test#{i}@test.com')")
            u = database.connection.from(:users).where(:name => "test #{i}").first
            database.connection.run(
              "insert into user_preferences (user_id, name, value) " +
              "values (#{u[:id]}, 'email', '#{u[:email]}')"
            )
            database.connection.run(
              "insert into user_preferences (user_id, name, value) " +
              "values (#{u[:id]}, 'name', '#{u[:name]}')"
            )
          end
        end

        describe "sanitizing subtable" do
          before(:each) do
            database.filter_table(:users) do |f|
              f.sanitize do |u|
                u[:email] = "blank_email#{u[:id]}@foo.bar"
                u
              end
              f.sanitize_subtable(:user_preferences,
                                  :foreign_key => :user_id,
                                  :local_field => :email,
                                  :foreign_field => :value,
                                  :type_key => :name,
                                  :type => 'email')
            end
            database.shrink!
          end

          it "should sanitize user preference emails" do
            remaining_preferences = database.connection.
              from(:user_preferences).where(:name => 'email').all
            remaining_values = remaining_preferences.map {|p| p[:value]}
            expect(remaining_values.grep(/blank_email/).size).to eq(20)
          end
          it "should not sanitize preferences with a different type" do
            remaining_preferences = database.connection.
              from(:user_preferences).where(:name => 'name').all
            remaining_values = remaining_preferences.map {|p| p[:value]}
            expect(remaining_values.grep(/blank_email/).size).to eq(0)
          end
        end
      end
    end


    describe "three table filter chain" do
      before(:all) do
        # Rspec doesn't want you using 'let' defined things in before(:all)
        connection = PgShrink::Database::Postgres.new(PgSpecHelper.pg_config).
                     connection
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
            database.connection.run(
              "insert into users (name, email) " +
              "values ('test #{i}', 'test#{i}@test.com')"
            )
            u = database.connection.from(:users).
              where(:name => "test #{i}").first
            (1..3).each do |j|
              database.connection.run(
                "insert into user_preferences (user_id, name) " +
                "values (#{u[:id]}, 'pref#{i}#{j}')"
              )
              pref = database.connection.from(:user_preferences).
                where(:name => "pref#{i}#{j}").first
              database.connection.run(
                "insert into user_preference_values " +
                "(user_preference_id, value) " +
                "values (#{pref[:id]}, 'val#{i}#{j}')"
              )
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
              f.filter_subtable(:user_preference_values,
                                :foreign_key => :user_preference_id)
            end

            database.filter!
          end
          it "filters users down to the one matching" do
            remaining_users = database.connection.from(:users).all
            expect(remaining_users.size).to  eq(1)
          end
          it "filters preferences to only those associated with the user" do
            remaining_user = database.connection.from(:users).first
            remaining_preferences = database.connection.
                                             from(:user_preferences).all
            expect(remaining_preferences.size).to eq(3)
            expect(remaining_preferences.map {|u| u[:user_id]}.uniq).
              to eq([remaining_user[:id]])
          end
          it "filters preference values to those associated with the " +
             "preferences remaining" do
            remaining_user = database.connection.from(:users).first
            remaining_preferences = database.connection.
              from(:user_preferences).all
            remaining_preference_values = database.
              connection.from(:user_preference_values).all
            expect(remaining_preference_values.size).to eq(3)
            expect(remaining_preference_values.map {|v|
              v[:user_preference_id]
            }).to match_array(remaining_preferences.map {|p| p[:id]})
          end
        end
      end
    end
  end
  describe "polymorphic foreign key subtables" do
    before(:all) do
      # Rspec doesn't want you using 'let' defined things in before(:all)
      connection = PgShrink::Database::Postgres.new(PgSpecHelper.pg_config).
                   connection
      PgSpecHelper.create_table(connection, :users,
                                {'name' => 'character varying(256)',
                                 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(connection, :preferences,
                                {'context_id' => 'integer',
                                 'context_type' => 'character varying(256)',
                                 'name' => 'character varying(256)',
                                 'value' => 'character varying(256)'})
    end
    describe "with 20 users, associated prefs, and prefs for different type" do
      before(:each) do
        PgSpecHelper.clear_table(database.connection, :users)
        PgSpecHelper.clear_table(database.connection, :preferences)
        (1..20).each do |i|
          database.connection.run(
            "insert into users (name, email) " +
            "values ('test #{i}', 'test#{i}@test.com')")
          u = database.connection.from(:users).where(:name => "test #{i}").first
          (1..3).each do |j|
            database.connection.run(
              "insert into preferences (context_id, context_type, name, value)"+
              " values (#{u[:id]}, 'User', 'pref#{i}', 'prefvalue#{i}')")
          end
          database.connection.run(
            "insert into preferences (context_id, context_type, name, value) " +
            "values(#{u[:id]}, 'OtherClass', 'pref#{i}', 'prefvalue#{i}')")
        end
      end

      describe "simple two table filtering" do
        before(:each) do
          database.filter_table(:users) do |f|
            f.filter_by do |u|
              u[:name] == "test 1"
            end
            f.filter_subtable(:preferences, :foreign_key => :context_id,
                              :type_key => :context_type, :type => 'User')
          end
          database.filter!
        end

        it "will filter prefs with context_type 'User'" do
          remaining_user = database.connection.from(:users).first
          remaining_preferences = database.connection.from(:preferences).
            where(:context_type => 'User').all
          expect(remaining_preferences.size).to eq(3)
          expect(remaining_preferences.map {|u| u[:context_id]}.uniq).
            to eq([remaining_user[:id]])
        end

        it "will not filter preferences without context_type user" do
          remaining_preferences = database.connection.from(:preferences).
            where(:context_type => 'OtherClass').all
          expect(remaining_preferences.size).to eq(20)
        end
      end

      describe "an extra layer of polymorphic subtables" do
        before(:all) do
          connection = PgShrink::Database::Postgres.new(PgSpecHelper.pg_config).
                       connection
          PgSpecHelper.create_table(connection, :preference_dependents,
                                    {'context_id' => 'integer',
                                     'context_type' => 'character varying(256)',
                                     'value' => 'character varying(256)'})
        end

        before(:each) do
          PgSpecHelper.clear_table(database.connection, :preference_dependents)
          prefs = database.connection.from(:preferences).all
          prefs.each do |pref|
            database.connection.run(
              "insert into preference_dependents " +
              "(context_id, context_type, value) " +
              "values (#{pref[:id]}, 'Preference', 'depvalue#{pref[:id]}')")

            database.connection.run(
              "insert into preference_dependents " +
              "(context_id, context_type, value) " +
              "values (#{pref[:id]}, 'SomeOtherClass', 'fakevalue#{pref[:id]}')")

          end

          database.filter_table(:users) do |f|
            f.filter_by do |u|
              u[:name] == "test 1"
            end
            f.filter_subtable(:preferences, :foreign_key => :context_id,
                              :type_key => :context_type, :type => 'User')
          end

          database.filter_table(:preferences) do |f|
            f.filter_subtable(:preference_dependents,
                              :foreign_key => :context_id,
                              :type_key => :context_type,
                              :type => 'Preference')
          end
          database.filter!
        end

        it "will filter preference dependents associated with preferences" do
          remaining_preferences = database.connection.from(:preferences).all
          remaining_dependents = database.connection.
            from(:preference_dependents).
            where(:context_type => 'Preference').all

          expect(remaining_dependents.size).to eq(remaining_preferences.size)
        end

        it "will not filter preference dependents with different type" do
          other_dependents = database.connection.
            from(:preference_dependents).
            where(:context_type => 'SomeOtherClass').all
          expect(other_dependents.size).to eq(80)
        end
      end
    end
  end
  describe "has_and_belongs_to_many join tables" do
    before(:all) do
      # Rspec doesn't want you using 'let' defined things in before(;all)
      connection = PgShrink::Database::Postgres.new(PgSpecHelper.pg_config).
                   connection
      PgSpecHelper.create_table(connection, :users,
                                {'name' => 'character varying(256)',
                                 'email' => 'character varying(256)'})
      PgSpecHelper.create_table(connection, :apartments_users,
                                {'user_id' => 'integer',
                                 'apartment_id' => 'integer'}, nil)
      PgSpecHelper.create_table(connection, :apartments,
                                {'name' => 'character varying(256)'})
    end

    describe "with 5 users, each with 2 apartments, and 1 apartment shared by all 5" do
      before(:each) do
        PgSpecHelper.clear_table(database.connection, :users)
        PgSpecHelper.clear_table(database.connection, :apartments_users)
        PgSpecHelper.clear_table(database.connection, :apartments)
        database.connection.run("insert into apartments (name) values ('shared_apt')")
        shared_apt = database.connection.from(:apartments).first
        (1..5).each do |i|
          database.connection.run(
            "insert into users (name, email) " +
            "values ('test #{i}', 'test#{i}@test.com')")
          u = database.connection.from(:users).where(:name => "test #{i}").first
          (1..2).each do |j|
            database.connection.run(
              "insert into apartments (name) values ('apartment #{i}#{j}')")
          end
          apartments = database.connection.from(:apartments).
            where(:name => ["apartment #{i}1", "apartment #{i}2"]).all
          ([shared_apt] + apartments).each do |apt|
            database.connection.run(
              "insert into apartments_users (user_id, apartment_id) " +
              "values (#{u[:id]}, #{apt[:id]})")
          end
        end
      end
      describe "With a simple cascading filter" do
        before(:each) do
          database.filter_table(:users) do |f|
            f.filter_by do |u|
              u[:name] == "test 1"
            end
            f.filter_subtable(:apartments_users,
                              :foreign_key => :user_id) do |t|
              t.filter_subtable(:apartments, :foreign_key => :id,
                                :primary_key => :apartment_id)
            end
          end
          database.filter_table(:apartments_users, :primary_key => false)
          database.shrink!
        end

        it "Should filter down apartments_users" do
          u = database.connection.from(:users).where(:name => "test 1").first
          remaining_join_table = database.connection.from(:apartments_users).all
          expect(remaining_join_table.size).to eq(3)
        end

        it "Should filter apartments as well" do
          remaining_join_table = database.connection.from(:apartments_users).all
          remaining_apartments = database.connection.from(:apartments).all
          expect(remaining_apartments.size).to eq(3)
        end
      end
    end
  end
end
