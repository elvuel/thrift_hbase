# ThriftHbase
wip: spike
gen-rb from Hbase 0.94.x
TODO: scannerOpenWithScan(...) specs
## Installation

Add this line to your application's Gemfile:

    gem 'thrift_hbase'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install thrift_hbase

## Usage

WIP

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Test

* for file in spec/*_spec.rb; do ruby $file; done *

http://hbase.apache.org/book/thrift.html
http://hbase.apache.org/book/client.filter.html
http://blog.monkeyz.eu/2012/05/
http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/thrift/doc-files/Hbase.html
http://archive.cloudera.com/cdh4/cdh/4/hbase/apidocs/org/apache/hadoop/hbase/thrift/generated/package-summary.html
http://people.apache.org/~stack/hbase-0.92.2-candidate-0/hbase-0.92.2/docs/apidocs/org/apache/hadoop/hbase/thrift/ThriftServer.HBaseHandler.html#getRowsWithColumns(java.nio.ByteBuffer, java.util.List, java.util.List)

HBASE 0.94.5
interface interface org.apache.hadoop.hbase.thrift.generated.Hbase$Iface
method:get ** (get cell[s])
method:compact **
method:enableTable  **
method:disableTable **
method:isTableEnabled **
method:majorCompact (ig)
method:getTableNames  **
method:getColumnDescriptors **
method:getTableRegions **
method:createTable **
method:deleteTable **
method:getVer (ig)
method:getVerTs (ig)
method:getRow **(using getRowWithColumns)
method:getRowWithColumns **
method:getRowTs (ig)
method:getRowWithColumnsTs (ig)
method:getRows (using getRowWithColumns)
method:getRowsWithColumns **
method:getRowsTs (ig)
method:getRowsWithColumnsTs (ig)
method:mutateRow **
method:mutateRowTs (ig)
method:mutateRows (ig)
method:mutateRowsTs (ig)
method:atomicIncrement **
method:deleteAll ** (delete row cells)
method:deleteAllTs (ig) delete row cells  with match
method:deleteAllRow **
method:increment (ig)
method:incrementRows (ig)
method:deleteAllRowTs (ig)
method:scannerOpenWithScan **
method:scannerOpen **
method:scannerOpenWithStop **
method:scannerOpenWithPrefix **
method:scannerOpenTs **
method:scannerOpenWithStopTs **
method:scannerGet
method:scannerGetList **
method:scannerClose  **
method:getRowOrBefore (ig)
method:getRegionInfo  (ig)
