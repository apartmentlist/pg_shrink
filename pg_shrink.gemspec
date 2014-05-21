# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pg_shrink/version'

Gem::Specification.new do |spec|
  spec.name          = "pg_shrink"
  spec.version       = PgShrink::VERSION
  spec.authors       = ["Kevin Ball"]
  spec.email         = ["kmball11@gmail.com"]
  spec.description   = "pg_shrink makes it simple to shrink and sanitize a psql database"
  spec.summary       = ""
  spec.homepage      = ""
  spec.license       = ""

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'pg'
  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
end
