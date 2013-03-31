# encoding: utf-8
require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../../lib/thrift_hbase', File.dirname(__FILE__))
require_relative '../spec_helper'

describe ThriftHbase::Scanner do
  describe 'scannerOpenWithScan' do
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
        if i < 5
          row.id = "spec#{i}"
        else
          row.id = "other#{i}"
        end
        row.columns = {
          'colf1:name' => ThriftHbase::Cell.new(value: "th-gem#{i}"),
          'colf1:kind' => ThriftHbase::Cell.new(value: "测试"),
          'colf1:kind_id' => ThriftHbase::Cell.new(value: 999),
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

    it 'filters' do

      # KeyOnlyFilter ()
      # FirstKeyOnlyFilter ()
      # PrefixFilter ('<prefix>')
      # ColumnPrefixFilter ('<col_f1>')
      # MultipleColumnPrefixFilter ('[col_f1, col_f2]')
      # ColumnCountGetFilter (<count>)
      # PageFilter ('<page_size>')
      # ColumnPaginationFilter (<limit>, <offset>)
      # InclusiveStopFilter (‘<stop_row_key>’)
      # TimeStampsFilter (<timestamp>, <timestamp>, ... ,<timestamp>)

      # RowFilter (<compareOp>, ‘<row_comparator>’)
      # QualifierFilter (<compareOp>,‘<qualifier_comparator>’)

    end # filters

    describe '#RowFilter' do
      it 'BinaryComparator binary' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'binary:other')"
        #t_scan.filterString = "RowFilter (=, 'binaryprefix:other')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 0
        scanner.close
      end # BinaryComparator binary

      it 'BinaryPrefixComparator binaryprefix' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'binaryprefix:other')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 5
        scanner.close
      end # BinaryPrefixComparator binaryprefix
      
      it 'RegexStringComparator regexstring' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'regexstring:sp*')"
        # with regexstring:*e* => exception
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 5
        scanner.close
      end # RegexStringComparator regexstring
      
      it 'SubStringComparator substring' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'substring:sp')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 5
        scanner.close

        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'substring:e')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 10
        scanner.close
      end # SubStringComparator substring
      
    end # '#RowFilter'
  end # 'scannerOpen'
end # ThriftHbase::Scanner