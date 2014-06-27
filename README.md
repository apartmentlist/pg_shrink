[![Build Status](https://travis-ci.org/apartmentlist/pg-shrink.svg?branch=master)](https://travis-ci.org/apartmentlist/pg-shrink)

# PgShrink

The pg_shrink tool makes it easy to shrink and sanitize a postgres database,
allowing you to specify custom filtering and sanitization via a simple 
DSL in a configuration file (Shrinkfile).

The pg_shrink tool takes two arguments, a url for a postgres database and
the path to a configuration file (will default to the Shrinkfile in the
current directory)

The simplest way to learn how to use pg_shrink is via an example.

## Usage

### Example Shrinkfile
This is a simple Ruby DSL that defines which tables are to be filtered and
sanitized in what way, and the relationships between those tables when filtering
or sanitization is to be propagated.

```ruby
filter_table :users do |f|
   f.filter_by 'id % 1000 = 0'
  
  f.sanitize do |u|
    u[:email] = "sanitized_email#{u[:id]}@fake.com"
    u
  end

  f.filter_subtable(:user_preferences, :foreign_key => :user_id)
end
```

This particular example will filter the users table to contain only users with
a name matching the regular expression /save me/, sanitize the email field on
those users, and then filter the user_preferences table to contain only
preferences associated with those users.

### Full DSL

See the Shrinkfile.example file in this directory for a complete list of the
available DSL.

### Options
```
-u, --url URL            *REQUIRED* Specify URL to postgres database.
                         WARNING: This database should be a backup and not
                         be changing at the time pg_shrink is run.  It will
                         be modified in place.
-c, --config SHRINKFILE  Specify a configuration file for how to shrink
--force                  Force run without confirmation.
-h, --help               Show this message and exit
```

## How does it work?

The pg_shrink command runs through 4 major steps.
* 1. Options parsing.
* 2. Shrinkfile parsing and setting up the structure of tables, filters, sanitizers,
and their subtable relationships
* 3. Iterating through tables and doing a depth-first filter on them.
* 4. Iterating through tables and doing a depth-first sanitization on them.

**Step 1:** Option parsing is simple. pg_shrink uses `optparse`

**Step 2:** Before anything is run, the Shrinkfile is completely parsed, setting up a set of tables, the filters and sanitizers on those tables, and any subtable relationships

**Step 3:** For each table, the filters on that table are iterated through.  For each filter, the records in the table are pulled out in batches, the filter is applied to that batch, and then any subtable filters are applied for records impacted within that batch.

**Step 4:** For each table, the sanitizers on that table are iterated through.  For each filter, the records in the table are pulled out in batches, the sanitizers is applied to that batch, and then any subtable sanitizers are applied for records impacted within that batch.

## Installation

Add this line to your application's Gemfile:

    gem 'pg_shrink'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install pg_shrink

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

