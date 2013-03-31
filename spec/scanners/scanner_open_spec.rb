# encoding: utf-8
require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../../lib/thrift_hbase', File.dirname(__FILE__))
require_relative '../spec_helper'

describe ThriftHbase::Scanner do
  describe 'scannerOpen' do
    before {
      @table_name = 'th_gem_table_test'
      @column_families = %w(colf1)
      @connection = ThriftHbase::Connection.new(
        host: TH_SPEC_CONFIG['host'],
        port: TH_SPEC_CONFIG['port']
      )
      @connection.create_table(@table_name, *@column_families.collect { |name|
        { name: name }
      })

      @table = ThriftHbase::Table.new(@connection, @table_name)

      10.times do |i|
        row = ThriftHbase::Row.new(table: @table)
        row.id = "spec#{i}"
        row.columns = {
          'colf1:name' => ThriftHbase::Cell.new(value: "th-gem#{i}"),
          'colf1:count' => ThriftHbase::Cell.new(value: i),
          'colf1:created_at' => ThriftHbase::Cell.new(value: Time.now.utc.to_s)
        }
        row.save
      end
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'scanning' do
      scanner = ThriftHbase::Scanner.new(@table, ['colf1'])
      scanner.open
      results = scanner.fetch_rows
      results.size.must_equal 10
      scanner.close

      scanner = ThriftHbase::Scanner.new(@table, ['colf1'])
      scanner.limit = 5
      scanner.open
      results = scanner.fetch_rows
      results.size.must_equal 5
      names = results.collect { |row| row.columns['colf1:name'].value }
      names.sort.must_equal (0..4).to_a.collect {|i| "th-gem#{i}" }
      scanner.close

      scanner = ThriftHbase::Scanner.new(@table, ['colf1'])
      scanner.options = { start_row: 'spec5'}
      scanner.open
      results = scanner.fetch_rows
      results.size.must_equal 5
      names = results.collect { |row| row.columns['colf1:name'].value }
      names.sort.must_equal (5..9).to_a.collect { |i| "th-gem#{i}" }
      scanner.close
    end # scanning
  end # 'scannerOpen'
end # ThriftHbase::Scanner