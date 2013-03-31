# encoding: utf-8
require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../../lib/thrift_hbase', File.dirname(__FILE__))
require_relative '../spec_helper'

describe ThriftHbase::Scanner do
  describe 'scannerOpenWithScan' do
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
      # FamilyFilter (<compareOp>,‘<family_comparator>’)
      # QualifierFilter (<compareOp>,‘<qualifier_comparator>’)
      # ValueFilter (<compareOp>,‘<value_comparator>’)

    end # filters

    describe '#RowFilter' do
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
      it 'BinaryComparator binary' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new

        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "RowFilter (=, 'binary:other')"
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

    describe 'FamilyFilter' do
      before do
        @table_name = 'th_gem_table_test'
        @column_families = %w(colf1 colf2)
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
            'colf1:kind' => ThriftHbase::Cell.new(value: "测试"),
            'colf1:kind_id' => ThriftHbase::Cell.new(value: 999),
            'colf1:count' => ThriftHbase::Cell.new(value: i),
            'colf1:created_at' => ThriftHbase::Cell.new(value: Time.now.utc.to_s)
          }
          row.save
        end

        5.times do |i|
          row = ThriftHbase::Row.new(table: @table)
          row.id = "colf2-row#{i}"
          row.columns = {
            'colf2:name' => ThriftHbase::Cell.new(value: "th-gem-colf2#{i}"),
            'colf2:kind' => ThriftHbase::Cell.new(value: "测试"),
            'colf2:kind_id' => ThriftHbase::Cell.new(value: 999),
            'colf2:count' => ThriftHbase::Cell.new(value: i),
            'colf2:created_at' => ThriftHbase::Cell.new(value: Time.now.utc.to_s)
          }
          row.save
        end
      end

      after do
        @connection.drop_table(@table_name)
        @connection.close
      end

      it 'return rows for the selected column family' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new
        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "FamilyFilter (=,'binary:colf2')"
        #t_scan.filterString = "FamilyFilter (=,'binaryprefix:colf2')" # the same
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 5
        results.each do |row|
          cf = row.columns.keys.map { |cf| cf.split(":").first }.uniq
          cf.must_equal ['colf2']
        end
        scanner.close
      end # return rows for the selected column family
    end # 'FamilyFilter'

    describe 'QualifierFilter' do
      before do
        @table_name = 'th_gem_table_test'
        @column_families = %w(colf1 colf2)
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
            'colf1:kind' => ThriftHbase::Cell.new(value: "测试"),
            'colf1:kind_id' => ThriftHbase::Cell.new(value: 999),
            'colf1:count' => ThriftHbase::Cell.new(value: i),
            'colf1:created_at' => ThriftHbase::Cell.new(value: Time.now.utc.to_s)
          }
          row.columns.update(
            {
              'colf1:name' => ThriftHbase::Cell.new(value: "th-gem#{i}"),
            }
          ) if i > 4
          row.save
        end
      end

      after do
        @connection.drop_table(@table_name)
        @connection.close
      end

      it 'return columns with the selected column' do
        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new
        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "QualifierFilter (=,'binaryprefix:kind')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 10
        scanner.close

        t_scan = Apache::Hadoop::Hbase::Thrift::TScan.new
        scanner = ThriftHbase::Scanner.new(@table, [], scanner_to_invoke: :with_scan)
        t_scan.filterString = "QualifierFilter (=,'binary:name')"
        scanner.options = {
          t_scan: t_scan
        }
        scanner.open
        results = scanner.fetch_rows
        results.size.must_equal 5
        scanner.close
      end # return columns with the selected family
    end # 'QualifierFilter'
  end # 'scannerOpen'
end # ThriftHbase::Scanner