# encoding: utf-8

require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../lib/thrift_hbase', File.dirname(__FILE__))
require_relative 'spec_helper'

describe ThriftHbase::Row do

  describe '#intialize' do
    it 'initialized instance variables' do
      row = ThriftHbase::Row.new(
        id: 'row_key', table: 'table', columns: {'cf:col1' => 'value'}
      )
      row.id.must_equal 'row_key'
      row.table.must_equal 'table'
      row.columns.must_equal({'cf:col1' => 'value'})
    end # initialized instance variables
  end # '#intialize'

  describe '#save' do
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

    it 'mutate row' do
      table = @connection.get_table(@table_name)
      row = ThriftHbase::Row.new(table: table)
      create_timestamp = Time.now.utc.to_s
      row.id = 'spec'
      row.columns = {
        'colf1:name' => ThriftHbase::Cell.new(value: 'th-gem'),
        'colf1:created_at' => ThriftHbase::Cell.new(value: create_timestamp),
        'colf1:count' => ThriftHbase::Cell.new(value: 1)
      }
      row.save
      row.get_cell('colf1:name').value.must_equal 'th-gem'
      row.get_cell('colf1:created_at').value.must_equal create_timestamp
      row.get_cell('colf1:no-column').must_be_nil
      ThriftHbase::Cell.hex_string_to_integer(
        row.get_cell('colf1:count').value
      ).must_equal 1


      ## get spec
      #new_row = row.get
      #new_row.must_be_instance_of ThriftHbase::Row
      #new_row.column_names.sort.must_equal %w(colf1:name colf1:created_at colf1:count).sort
      #new_row.columns['colf1:name'].value='then-gem-updated'
      #new_row.save
      #row.get_cell('colf1:name').value.must_equal 'then-gem-updated'

      # get with columns spec
      new_row = row.get(row.id, %w(colf1:name colf1:count))
      new_row.must_be_instance_of ThriftHbase::Row
      new_row.column_names.wont_include 'colf1:created_at'
      new_row.column_names.sort.must_equal %w(colf1:name colf1:count).sort

      new_row = row.get(row.id)
      new_row.must_be_instance_of ThriftHbase::Row
      new_row.column_names.sort.must_equal %w(colf1:name colf1:created_at colf1:count).sort
      new_row.columns['colf1:name'].value='then-gem-updated'
      new_row.save
      row.get_cell('colf1:name').value.must_equal 'then-gem-updated'

    end # mutate row
  end # '#save'

  describe '#delete_cells' do
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

      row = ThriftHbase::Row.new(table: @table)
      row.id = 'spec'
      row.columns = {
        'colf1:name' => ThriftHbase::Cell.new(value: 'th-gem'),
        'colf1:count' => ThriftHbase::Cell.new(value: 1)
      }
      row.save
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'should delete the specific cells' do
      row = @table.find('spec').first
      row.columns['colf1:name'].value.must_equal 'th-gem'
      ThriftHbase::Cell.hex_string_to_integer(
        row.columns['colf1:count'].value
      ).must_equal 1

      row.delete_cells('colf1:name').must_equal true

      row_refresh = @table.find('spec').first
      row_refresh.column_names.must_equal ['colf1:count']
      row_refresh.get_cell('colf1:name').must_be_nil
      ThriftHbase::Cell.hex_string_to_integer(
        row_refresh.get_cell('colf1:count').value
      ).must_equal 1

    end # should delete the specific cells
  end # '#delete_cells'

  describe '#atomic_increment' do
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

      row = ThriftHbase::Row.new(table: @table)
      row.id = 'spec'
      row.columns = {
        'colf1:name' => ThriftHbase::Cell.new(value: 'th-gem'),
        'colf1:count' => ThriftHbase::Cell.new(value: 0)
      }
      row.save
    }
    after {
      @connection.drop_table(@table_name)
      @connection.close
    }

    it 'should increment the column value' do
      row = @table.find('spec').first
      ThriftHbase::Cell.hex_string_to_integer(
        row.columns['colf1:count'].value
      ).must_equal 0

      row.get_cell('colf1:name').value.must_equal 'th-gem'

      row.atomic_increment('colf1:count', 2)
      ThriftHbase::Cell.hex_string_to_integer(
        row.get_cell('colf1:count').value
      ).must_equal 2

      row.atomic_increment('colf1:name', 1)
      row.get_cell('colf1:name').value.must_equal 'th-gem'
    end # should increment the column value

  end # '#atomic_increment'
end