# encoding: utf-8
module ThriftHbase
  class Row

    def self.populate_from_t_row_result(result, table)
      row                 = self.new(id: result.row, table: table)
      row.new_record      = false
      result.columns.each do |name, value|
        row.columns[name] =  ThriftHbase::Cell.populate_from_tcell(value)
      end
      row
    end

    attr_accessor :id, :columns, :table, :new_record

    def initialize(options = {})
      @id         = options[:id]
      @table      = options[:table]
      @columns    = options[:columns] || {}
      @new_record = true
    end

    def row_key
      self.id
    end

    def row_key=(value)
      self.id = value
    end

    def new_record?
      @new_record
    end

    def column_families
      table.column_families
    end

    def column_family_names
      table.column_family_names
    end

    def column_names
      columns.keys
    end

    # if column value is nil, just return an empty array
    # ThriftBase::Cell.hex_string_to_integer('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001')
    ## TODO investigate how to get celll's column name
    ## buffer_transport.rb #read(sz)
    #def get_cells(column)
    #  table.client.get(table.name, id, column, {}).map do |tcell|
    #    ThriftHbase::Cell.populate_from_tcell(tcell)
    #  end
    #end
    def get_cell(column)
      tcell = table.client.get(table.name, id, column, {}).first
      tcell.nil? ? nil : ThriftHbase::Cell.populate_from_tcell(tcell)
    end

    def delete_cells(column)
      table.client.deleteAll(table.name, id, column, {}).nil?
    end

    def atomic_increment(column, value=1)
      table.client.atomicIncrement(table.name, id, column, value)
    end

    #def get(row = id)
    #  results = table.client.getRow(table.name, row, {})
    #  self.class.populate_from_t_row_result(results.first, table) unless results.empty?
    #end

    def get(row, columns=[])
      results = table.client.getRowWithColumns(table.name, row, columns, {})
      self.class.populate_from_t_row_result(results.first, table) unless results.empty?
    end

    def save
      mutations = []
      @columns.each do |column_name, cell|
        mutations << Apache::Hadoop::Hbase::Thrift::Mutation.new(
          column: column_name, value: cell.value_to_thrift, isDelete: cell.value.nil?
        )
      end
      table.client.mutateRow(
        table.name,
        #id.to_s.dup.force_encoding(Encoding::BINARY),
        id,
        mutations,
        {}
      )
    end

    def destroy
      table.client.deleteAllRow(table.name, id, {}).nil?
    end

  end # Row
end # ThriftHbase