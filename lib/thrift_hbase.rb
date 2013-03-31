# encoding: utf-8
require 'thrift'

require_relative 'thrift_hbase/version'

# Thrift ruby
require_relative 'gen-rb/hbase_types'
require_relative 'gen-rb/hbase.rb'
require_relative 'gen-rb/hbase_constants'

require_relative 'thrift_hbase/connection'
require_relative 'thrift_hbase/table'
require_relative 'thrift_hbase/row'
require_relative 'thrift_hbase/cell'
require_relative 'thrift_hbase/scanner'

# thrift monkey non-english
module Thrift
  class BinaryProtocol < BaseProtocol
    def write_string(str)
      begin
        str = Bytes.convert_to_utf8_byte_buffer(str)
      rescue
      end
      write_i32(str.length)
      trans.write(str)
    end
  end
end

module ThriftHbase
  # Your code goes here...
end
