require 'pg'
require 'sequel'
require 'active_support/core_ext/hash'

module PgSpecHelper

  # TODO:  Should probably make the db name and user (and other access stuff in
  # test) easily configurable.  Or set up a vagrant instance to completely
  # standardize.
  def self.create_database(db_name = 'test_pg_shrink', user = 'postgres')
    `psql --username=#{user} --command="create database #{db_name};"`
  end

  def self.drop_table(table, db_name = 'test_pg_shrink', user = 'postgres')
    db = Sequel.connect("postgres://#{user}@localhost/#{db_name}")
    db.run("drop table if exists #{table}")
  end
  def self.create_table(table, columns = {}, primary_key = :id, db_name = 'test_pg_shrink', user = 'postgres')
    primary_key = primary_key.to_sym
    columns = {primary_key=> 'serial primary key'}.merge(columns.symbolize_keys)
    sql = "create table #{table} (" +
      columns.map {|col, type| "#{col} #{type}"}.join(',') +
      ")"
    db = Sequel.connect("postgres://#{user}@localhost/#{db_name}")
    db.run(sql)
  end

  def self.clear_table(table, db_name = 'test_pg_shrink', user = 'postgres')
    db = Sequel.connect("postgres://#{user}@localhost/#{db_name}")
    db.run("delete from #{table};")
  end

end
