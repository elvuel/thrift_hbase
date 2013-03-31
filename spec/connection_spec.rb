# encoding: utf-8
require 'minitest/autorun'
require 'ostruct'
require 'mocha/setup'
require File.expand_path('../lib/thrift_hbase', File.dirname(__FILE__))
require_relative 'spec_helper'

describe ThriftHbase::Connection do

  describe '#initialize' do

    it 'take one argument' do
      lambda { ThriftHbase::Connection.new('arg1', 'arg2') }.must_raise ArgumentError
    end # at least one argument

    it 'instantiated host' do
      connection = ThriftHbase::Connection.new
      connection.host.must_equal 'localhost'

      connection = ThriftHbase::Connection.new(host: 'hbase_thrift_host')
      connection.host.must_equal 'hbase_thrift_host'
    end # instantiated host

    it 'instantiated port' do
      connection = ThriftHbase::Connection.new
      connection.port.must_equal 9090

      connection = ThriftHbase::Connection.new(host: 'hbase_thrift_host', port: 19090)
      connection.port.must_equal 19090
    end # instantiated port

    it 'instantiated default timeout' do
      connection = ThriftHbase::Connection.new
      connection.timeout.must_equal 5000
    end # instantiated timeout
  end # '#initialize'

  describe '#transport' do

    before do
      @connection = ThriftHbase::Connection.new
    end

    it 'must be instance of Thrift::BufferedTransport' do
      @connection.transport.must_be_instance_of ::Thrift::BufferedTransport
    end # must be instance of Thrift::BufferedTransport

    it 'the initial transport open state must be false' do
      @connection.transport.open?.must_equal false
    end # the initial transport open state must be false
  end # '#transport'

  describe '#open' do

    it 'raise error if transport open failed' do
      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.open; raise RuntimeError end
      connection.stubs(:transport).returns(transport)
      lambda { connection.open }.must_raise RuntimeError
    end # raise error if transport open failed

    it 'returns true if transport open successfully' do
      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.open; "opened" end
      connection.stubs(:transport).returns(transport)
      connection.open.must_equal true
    end # returns true if transport open successfully

    it 'the @client instance variable should be initialized' do
      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.open; "opened" end
      connection.stubs(:transport).returns(transport)
      connection.open
      connection.instance_variable_get(:@client)
      .must_be_instance_of Apache::Hadoop::Hbase::Thrift::Hbase::Client
    end # the @client instance variable should be initialized
  end # '#open'

  describe '#close' do
    it 'returns boolean' do
      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.close; nil end
      connection.stubs(:transport).returns(transport)
      connection.open
      connection.close.must_equal true

      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.close; "nil" end
      connection.stubs(:transport).returns(transport)
      connection.open
      connection.close.must_equal false
    end # returns boolean
  end # '#close'

  describe '#open?' do
    it 'returns boolean' do
      connection = ThriftHbase::Connection.new
      connection.open?.must_equal false

      connection = ThriftHbase::Connection.new
      transport = OpenStruct.new
      def transport.open?; true end
      connection.stubs(:transport).returns(transport)
      connection.open?.must_equal true
    end # returns boolean
  end

  describe '#client' do
    it 'returns the @client instance variable' do
      connection = ThriftHbase::Connection.new
      connection.instance_variable_set(:@client, 'client')
      connection.client.must_equal 'client'
    end # returns the @client instance variable
  end # '#client'

  describe '#tables' do
    it 'list all tables' do
      connection = ThriftHbase::Connection.new
      client = OpenStruct.new
      def client.getTableNames; %w(table1 table2) end
      connection.stubs(:client).returns(client)
      connection.tables.must_equal %w(table1 table2)
    end # list all tables
  end # '#tables'

  describe '#has_table?' do
    it 'return the give table name existed or not' do
      connection = ThriftHbase::Connection.new
      client = OpenStruct.new
      def client.getTableNames; %w(table1 table2) end
      connection.stubs(:client).returns(client)
      connection.tables.must_equal %w(table1 table2)
      connection.has_table?('table3').must_equal false
      connection.has_table?('table2').must_equal true
    end # return the give table name existed or not
  end # '#has_table?'

  #describe 'Real hbase env' do
  #  unless TH_SPEC_CONFIG.empty?
  #    it 'should test with open close' do
  #      connection = ThriftHbase::Connection.new(
  #        host: TH_SPEC_CONFIG['host'],
  #        port: TH_SPEC_CONFIG['port']
  #      )
  #      connection.open?.must_equal false
  #      connection.client.must_be_nil
  #      connection.open.must_equal true
  #      connection.open?.must_equal true
  #      connection.client.wont_be_nil
  #      connection.close.must_equal true
  #      connection.open?.must_equal false
  #    end # should test with open close
  #
  #    it 'with table operations' do
  #      connection = ThriftHbase::Connection.new(
  #        host: TH_SPEC_CONFIG['host'],
  #        port: TH_SPEC_CONFIG['port']
  #      )
  #      table_name = 'table_test'
  #      connection.create_table(table_name, name: 'colf1')
  #      connection.enable_table(table_name)
  #      connection.table_enabled?(table_name).must_equal true
  #      connection.disable_table(table_name)
  #      connection.table_enabled?(table_name).must_equal false
  #      connection.enable_table(table_name)
  #      connection.table_enabled?(table_name).must_equal true
  #      connection.disable_table(table_name)
  #      connection.tables.must_include table_name
  #      connection.drop_table(table_name)
  #      connection.tables.wont_include table_name
  #      connection.close
  #    end # create table
  #  end
  #
  #end # 'Real hbase env'

end # ThriftHbase::Connection