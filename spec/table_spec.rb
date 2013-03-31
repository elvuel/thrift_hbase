# encoding: utf-8

require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../lib/thrift_hbase', File.dirname(__FILE__))
require_relative 'spec_helper'

describe ThriftHbase::Table do
  describe '#initialize' do
    it 'should raise ArgumentError' do
      table = ThriftHbase::Table
      lambda { table.new }.must_raise ArgumentError
      lambda { table.new(OpenStruct.new) }.must_raise ArgumentError
      lambda { table.new(OpenStruct.new, OpenStruct.new, OpenStruct.new) }
      .must_raise ArgumentError
    end # should raise ArgumentError
  end # '#initialize'

  describe '#column_familes' do
    before {
      @table_name = 'th_gem_table_test'
      @column_families = %w(colf1 colf2)
      @connection = ThriftHbase::Connection.new(
        host: TH_SPEC_CONFIG['host'],
        port: TH_SPEC_CONFIG['port']
      )
      @connection.create_table(@table_name, *@column_families.collect { |name|
        { name: name }
      })

    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'returns the table column families' do
      table = ThriftHbase::Table.new(@connection, @table_name)
      table.column_family_names.sort.must_equal @column_families.sort
    end # returns the table column families
  end # '#column_familes'

  describe '#regions' do
    before {
      @table_name = 'th_gem_table_test'
      @column_families = %w(colf1 colf2)
      @connection = ThriftHbase::Connection.new(
        host: TH_SPEC_CONFIG['host'],
        port: TH_SPEC_CONFIG['port']
      )
      @connection.create_table(@table_name, *@column_families.collect { |name|
        { name: name }
      })
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'returns the table regions' do
      table = ThriftHbase::Table.new(@connection, @table_name)
      keys = %w(start_key end_key id name version).map(&:to_sym)
      regions = table.regions
      regions.must_be_instance_of Array
      regions.each { |region|
        region.must_be_instance_of Hash
        region.keys.sort.must_equal keys.sort
      }
    end # returns the table regions
  end # '#regions'

  describe '#compact' do
    before {
      @table_name = 'th_gem_table_test'
      @column_families = %w(colf1 colf2)
      @connection = ThriftHbase::Connection.new(
        host: TH_SPEC_CONFIG['host'],
        port: TH_SPEC_CONFIG['port']
      )
      @connection.create_table(@table_name, *@column_families.collect { |name|
        { name: name }
      })
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'returns the table or region compact' do
      table = ThriftHbase::Table.new(@connection, @table_name)
      table.compact.must_be_nil
      table.compact(table.regions.first[:name]).must_be_nil
    end #
  end # '#compact'

  describe '#find' do
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
      table = ThriftHbase::Table.new(@connection, @table_name)

      10.times do |i|
        row = ThriftHbase::Row.new(table: table)
        row.id = "spec#{i}"
        row.columns = {
          'colf1:name' => ThriftHbase::Cell.new(value: "th-gem-#{i}"),
          'colf1:count' => ThriftHbase::Cell.new(value: i)
        }
        row.save
      end
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }
    
    it 'returns the query rows' do
      table = ThriftHbase::Table.new(@connection, @table_name)
      results = table.find([])
      results.must_be_instance_of Array
      results.must_be_empty

      results = table.find(%w(spec0 spec6 spec4))
      results.must_be_instance_of Array
      results.size.must_equal 3

      results.collect do |row|
        row.columns['colf1:name'].value
      end.sort.must_equal %w(th-gem-0 th-gem-4 th-gem-6)

      results.collect do |row|
        ThriftHbase::Cell.hex_string_to_integer(row.columns['colf1:count'].value)
      end.sort.must_equal [0, 4, 6]

      results = table.find(%w(spec0 spec6 spec-not))
      results.must_be_instance_of Array
      results.size.must_equal 2

      results.collect do |row|
        row.columns['colf1:name'].value
      end.sort.must_equal %w(th-gem-0 th-gem-6)

      results = table.find(%w(spec0 spec6 spec4), select: ['colf1:name'])
      results.must_be_instance_of Array
      results.size.must_equal 3

      results.each do |row|
        row.columns.keys.must_equal ['colf1:name']
      end

    end # 
    
  end # '#find'

end