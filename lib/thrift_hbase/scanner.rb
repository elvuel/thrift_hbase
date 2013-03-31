# encoding: utf-8
module ThriftHbase
  class Scanner

    INVOKE_MAPPING = {
      default: :scannerOpen,
      with_scan: :scannerOpenWithScan,
      open_with_stop: :scannerOpenWithStop,
      open_with_prefix: :scannerOpenWithPrefix
      #open_ts: :scannerOpenTs,
      #open_with_stop_ts: :scannerOpenWithStopTs
    }

    attr_accessor :table, :column_family_names, :opened_scanner
    attr_accessor :scanner_to_invoke, :options, :limit
    attr_accessor :formatted_column_family_names

    def initialize(table, column_family_names, options = {})
      @table               = table
      @column_family_names = column_family_names.collect{|n| n.split(":").first}
      @column_family_names = options.delete(:columns) unless options[:columns].nil?
      @formatted_column_family_names = @column_family_names.collect{|n| "#{n.split(":").first}:"}
      @scanner_to_invoke = (options.delete(:scanner_to_invoke) || 'default').to_sym
      @limit             = options.delete(:limit) || 20
      @options           = options
    end

    def open
      self.opened_scanner = table.client.send(to_invoke, *invoke_args)
    end

    def close
      table.client.scannerClose(opened_scanner)
    end

    def get
      talbe.client.scannerGet(opened_scanner)
    end

    def fetch_t_rows
      table.client.scannerGetList(opened_scanner, limit)
    end

    def fetch_rows
      populate_rows(fetch_t_rows)
    end

    def populate_rows(results)
      results.collect { |result| populate_row(result) }.compact
    end

    def populate_row(result)
      ThriftHbase::Row.populate_from_t_row_result(result, table)
    end

    private
    def to_invoke
      INVOKE_MAPPING.fetch(scanner_to_invoke)
    end

    def invoke_args
      args = [table.name]
      case scanner_to_invoke
        when :default
          args.push(options[:start_row] || '')
          args.push(formatted_column_family_names)
        when :with_scan
          args.push(options.fetch(:t_scan))
          args.push(formatted_column_family_names)
        when :open_with_stop
          args.push(options[:start_row] || '')
          args.push(options[:stop_row] || '')
          args.push(formatted_column_family_names)
        when :open_with_prefix
          args.push(options[:start_and_prefix] || '')
          args.push(formatted_column_family_names)
        #when :open_ts
        #  args.push(options[:start_row] || '')
        #  args.push(formatted_column_family_names)
        #  args.push(options[:timestamp]) # i64
        #when :open_with_stop_ts
        #  args.push(options[:start_row] || '')
        #  args.push(options[:stop_row] || '')
        #  args.push(formatted_column_family_names)
        #  args.push(options[:timestamp]) # i64
      end
      args.push({})
    end

  end # Scanner
end # ThriftHbase
