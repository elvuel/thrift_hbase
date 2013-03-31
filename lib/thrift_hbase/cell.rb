# encoding: utf-8

# ref: massive_record wrapper cell
module ThriftHbase
  class Cell
    SUPPORTED_TYPES = [NilClass, String, Fixnum, Bignum]

    attr_reader :value
    attr_accessor :created_at

    #
    # Packs an integer as a 64-bit signed integer, native endian (int64_t)
    # Reverse it as the byte order in hbase are reversed
    #
    def self.integer_to_hex_string(int)
      [int].pack('q').reverse
    end

    #
    # Unpacks an string as a 64-bit signed integer, native endian (int64_t)
    # Reverse it before unpack as the byte order in hbase are reversed
    #
    def self.hex_string_to_integer(string)
      string.reverse.unpack("q*").first
    end


    def self.populate_from_tcell(tcell)
      new({
            value: tcell.value,
            created_at: Time.at(tcell.timestamp / 1000, (tcell.timestamp % 1000) * 1000)
          })
    end

    def to_i16
      val, = value.unpack('n')
      (val > 0x7fff) ? (0 - ((val - 1) ^ 0xffff)) : val
    end

    def to_i32
      val, = value.unpack('N')
      (val > 0x7fffffff) ? (0 - ((val - 1) ^ 0xffffffff)) : val
    end

    def to_i64
      hi, lo = value.unpack('N2')
      if (hi > 0x7fffffff)
        hi ^= 0xffffffff
        lo ^= 0xffffffff
        0 - (hi << 32) - lo - 1
      else
        (hi << 32) + lo
      end
    end

    def to_double
      value.unpack('G').first
    end

    def initialize(options = {})
      self.value      = options[:value]
      self.created_at = options[:created_at]
    end

    def value=(value)
      raise "#{value} was a #{value.class}, but it must be a one of: #{SUPPORTED_TYPES.join(', ')}" unless SUPPORTED_TYPES.include? value.class

      #@value = value.duplicable? value.dup : value
      @value = value
    end

    def value_to_thrift
      case value
        when String
          value.force_encoding(Encoding::BINARY)
        when Fixnum, Bignum
          self.class.integer_to_hex_string(value)
        when NilClass
          value
      end
    end

  end # Cell
end # ThriftHbase