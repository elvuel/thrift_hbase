# encoding: utf-8

module ThriftHbase
  class Table
    attr_reader :name, :connection
    def initialize(connection, name)
      @connection = connection
      @name       = name
    end

    def enable!
      connection.enable_table(name)
    end

    def disable!
      connection.disable_table(name)
    end

    def enabled?
      connection.table_enable?(name)
    end

    def destroy
      connection.drop_table(name)
    end

    def client
      connection.client
    end

    def column_families
      @column_families ||= client.getColumnDescriptors(name)
    end

    def column_family_names
      column_families.keys.map { |cf| cf.split(':').first }.compact
    end

    def regions
      client.getTableRegions(name).map do |region|
        {
          start_key: region.startKey,
          end_key: region.endKey,
          id: region.id,
          name: region.name,
          version: region.version
        }
      end
    end

    def compact(table_or_region_name=name)
      client.compact(table_or_region_name)
    end

    def find(rows, options = {})
      rows = [rows] unless rows.is_a?(Array)
      columns = options[:select] || []
      collection = client.getRowsWithColumns(name, rows, columns, {})
      collection.collect do |result|
        ThriftHbase::Row.populate_from_t_row_result(result, self)
      end
    end

  end # Table
end # ThriftHbase