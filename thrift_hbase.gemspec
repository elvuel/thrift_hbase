# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'thrift_hbase/version'

Gem::Specification.new do |gem|
  gem.name          = "thrift_hbase"
  gem.version       = ThriftHbase::VERSION
  gem.authors       = ["elvuel"]
  gem.email         = ["elvuel@gmail.com"]
  gem.description   = %q{Ruby HBase library using Thrift}
  gem.summary       = %q{HBase via Ruby and Thrift}
  gem.homepage      = "http://github.com/elvuel"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency 'thrift', '>= 0.9.0'
  gem.add_development_dependency 'minitest', '>= 4.4.0'
  gem.add_development_dependency 'mocha', '>= 0.13.0'
end
