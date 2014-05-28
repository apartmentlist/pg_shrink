require 'pg'
require 'sequel'
require 'active_support/core_ext/hash'

module PgSpecHelper

  # TODO:  Make the db name and user (and other access stuff in
  # test) easily configurable.
  def self.reset_database(db_name = 'test_pg_shrink', user = 'postgres')
    `psql --username=#{user} --command="drop database #{db_name};"`
    `psql --username=#{user} --command="create database #{db_name};"`
  end

  def self.drop_table_if_exists(connection, table)
    connection.run("drop table if exists #{table}") 
  end

  def self.create_table(connection, table, columns = {}, primary_key = :id)
    # For ease of testing, whenever we create we want to override any previous
    # tables
    self.drop_table_if_exists(connection, table)
    columns = if primary_key
      primary_key = primary_key.to_sym
      columns = {primary_key=> 'serial primary key'}.merge(columns.symbolize_keys)
    else
      columns.symbolize_keys
    end
    sql = "create table #{table} (" +
      columns.map {|col, type| "#{col} #{type}"}.join(',') +
      ")"
    connection.run(sql)
  end

  def self.clear_table(connection, table)
    connection.run("delete from #{table};")
  end

end
