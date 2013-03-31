# encoding: utf-8

module ThriftHbase
  class Connection
    attr_accessor :host, :port, :timeout

    def initialize(options = {})
      options = {} unless options.is_a?(Hash)
      @host    = options[:host] || 'localhost'
      @port    = options[:port] || 9090
      @timeout = 5000
    end

    def transport
      @transport ||= ::Thrift::BufferedTransport.new(::Thrift::Socket.new(@host, @port, @timeout))
    end

    def open
      protocol = ::Thrift::BinaryProtocol.new(transport)
      @client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)
      begin
        transport.open
        true
      rescue
        raise RuntimeError, "Unable to connect to HBase on #{host}, port #{port}"
      end
    end

    def close
      transport.close.nil?
    end

    def open?
      transport.open?
    end

    def client
      open unless @client
      @client
    end

    def tables
      client.getTableNames
    end

    def has_table?(name)
      tables.include?(name)
    end

    # add column family to table(Hbase shell)
    # disable table
    # alter table, {NAME => 'new_colfamily'}
    # enable table
    def create_table(name, *column_family_names)
      unless has_table?(name)
        column_family_names.map! { |descriptor|
          Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(descriptor)
        }
        client.createTable(name, column_family_names)
      end
      get_table(name)
    end

    def get_table(name)
      raise StandardError, "No such table #{name}" unless has_table?(name)
      ThriftHbase::Table.new(self, name)
    end

    def enable_table(name)
      client.enableTable(name) unless table_enabled?(name)
    end

    def disable_table(name)
      client.disableTable(name) if table_enabled?(name)
    end

    def drop_table(name)
      disable_table(name)
      client.deleteTable(name)
    end

    def table_enabled?(name)
      raise StandardError, "No such table #{name}" unless has_table?(name)
      client.isTableEnabled(name)
    end

    def reconnect!
      @transport = nil
      @client    = nil
      open
    end

    def method_missing(method, *args)
      begin
        open unless client
        client.send(method, *args) if client
      rescue ::Thrift::TransportException => error
        reconnect!
        client.send(method, *args) if client
      end
    end

  end # Connection
end # ThriftHbase