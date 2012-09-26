package shark.memstore

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hive.serde2.{ByteStream, SerDe}
import org.apache.hadoop.hive.serde2.`lazy`.{LazyFactory, LazySimpleSerDe}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.{ListObjectInspector, MapObjectInspector,
  ObjectInspector, PrimitiveObjectInspector, StructObjectInspector, UnionObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Writable

import scala.collection.JavaConversions._

import shark.{LogHelper, SharkConfVars}


class ColumnarSerDe extends SerDe with LogHelper {

  var cachedObjectInspector: StructObjectInspector = _
  var cachedWritable: ColumnarWritable = _
  var cachedStruct: ColumnarStruct = _
  var serDeParams: SerDeParameters = _
  var initialColumnSize: Int = _
  val serializeStream = new ByteStream.Output()

  def columnFactory: ColumnFactory = Column

  override def initialize(conf: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName())
    // Create oi & writable.
    cachedObjectInspector = ColumnarStructObjectInspector(serDeParams)

    // This null check is needed because Hive's SemanticAnalyzer.genFileSinkPlan() creates
    // an instance of the table's StructObjectInspector by creating an instance SerDe, which
    // it initializes by passing a 'null' argument for 'conf'.
    if (conf != null) {
      initialColumnSize = SharkConfVars.getIntVar(conf, SharkConfVars.COLUMN_INITIALSIZE)

      if (initialColumnSize == - 1) {
        // Approximate the size of a partition by using the HDFS "dfs.block.size" config.
        // Try both "dfs.block.size" and "dfs.blocksize" (from DFS_BLOCK_SIZE_KEY), which
        // is the new name since Hadoop 0.21.0
        val partitionSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
          conf.getLong("dfs.block.size", DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT))

        logDebug("Estimated size of partition to cache is " + partitionSize)

        // Estimate the initial capacity for ArrayList columns using:
        // partition_size / (num_columns * avg_field_size).
        val rowSize = ColumnarSerDe.getFieldSize(cachedObjectInspector).toLong
        initialColumnSize = (partitionSize / rowSize).toInt

        logDebug("Estimated size of each row is: " + rowSize)
      }
    }

    cachedWritable = new ColumnarWritable(cachedObjectInspector, initialColumnSize, columnFactory)
  }

  override def deserialize(blob: Writable): Object = {
    if (cachedStruct == null)
      cachedStruct = new ColumnarStruct(blob.asInstanceOf[ColumnarWritable])
    cachedStruct.initializeNextRow()
    cachedStruct
  }

  override def getObjectInspector(): ObjectInspector = {
    return cachedObjectInspector
  }

  override def getSerializedClass(): Class[_ <: Writable] = {
    return classOf[ColumnarWritable]
  }

  override def serialize(obj: Object, objInspector: ObjectInspector): Writable = {

    val soi = objInspector.asInstanceOf[StructObjectInspector]
    val fields = soi.getAllStructFieldRefs

    var i = 0
    while (i < fields.size) {
      val field = fields.get(i)
      val fieldOI = field.getFieldObjectInspector
      fieldOI.getCategory match {
        case ObjectInspector.Category.PRIMITIVE =>
          cachedWritable.add(i, soi.getStructFieldData(obj, field), fieldOI)
        case other => {
          // We use LazySimpleSerDe to serialize nested data
          LazySimpleSerDe.serialize(
            serializeStream, soi.getStructFieldData(obj, field),
            fieldOI,
            serDeParams.getSeparators(),
            1,
            serDeParams.getNullSequence(),
            serDeParams.isEscaped(),
            serDeParams.getEscapeChar(),
            serDeParams.getNeedsEscape())
          cachedWritable.add(i, serializeStream, fieldOI)
          serializeStream.reset()
        }
      }
      i += 1
    }
    cachedWritable
  }
}


class ColumnarSerDeWithStats extends ColumnarSerDe {
  override def columnFactory: ColumnFactory = ColumnWithStats

  def stats: TableStats = {
    new TableStats(cachedWritable.columns.map {
      case column: ColumnWithStats[_] => Some(column.stats)
      case _ => None
    })
  }
}


// Companion object, used to determine the row size, which is then used to
// determine the initial capacity to allocate as fastutil buffer.
object ColumnarSerDe {

  // Sizes of primitive types
  val BOOLEAN_SIZE = 1
  val BYTE_SIZE = 1
  val SHORT_SIZE = 2
  val INT_SIZE = 4
  val LONG_SIZE = 8
  val FLOAT_SIZE = 4
  val DOUBLE_SIZE = 8

  // Strings are assumed to be 16 bytes on average.
  val STRING_SIZE = 16

  // Void columns are represented by NullWritable, which is a singleton, so it's not
  // large enough to matter.
  val NULL_SIZE = 0

  // Estimates for average sizes of Lists and Maps.
  val MAP_SIZE = 5
  val LIST_SIZE = 5

  // Determine average field size from the ObjectInspector passed. Should remove parts for
  // nested data once those are supported.
  def getFieldSize(oi: ObjectInspector): Int = {
    val size = oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => BOOLEAN_SIZE
          case PrimitiveCategory.BYTE => BYTE_SIZE
          case PrimitiveCategory.SHORT => SHORT_SIZE
          case PrimitiveCategory.INT => INT_SIZE
          case PrimitiveCategory.LONG => LONG_SIZE
          case PrimitiveCategory.FLOAT => FLOAT_SIZE
          case PrimitiveCategory.DOUBLE => DOUBLE_SIZE
          case PrimitiveCategory.STRING => STRING_SIZE
          case PrimitiveCategory.VOID => NULL_SIZE
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case ObjectInspector.Category.LIST => {
        val listOISize = getFieldSize(
          oi.asInstanceOf[ListObjectInspector].getListElementObjectInspector())
        LIST_SIZE * listOISize
      }
      case ObjectInspector.Category.STRUCT => {
        val fieldRefs = oi.asInstanceOf[StructObjectInspector].getAllStructFieldRefs()
        fieldRefs.foldLeft(0)((sum, structField) =>
          sum + getFieldSize(structField.getFieldObjectInspector))
      }
      case ObjectInspector.Category.UNION => {
        val unionOIs = oi.asInstanceOf[UnionObjectInspector].getObjectInspectors()
        val unionOISizes = unionOIs.foldLeft(0)((sum, unionOI) =>
          sum + getFieldSize(unionOI))
        unionOISizes / unionOIs.size
      }
      case ObjectInspector.Category.MAP => {
        val mapOI = oi.asInstanceOf[MapObjectInspector]
        val keySize = getFieldSize(mapOI.getMapKeyObjectInspector())
        val valueSize = getFieldSize(mapOI.getMapValueObjectInspector())
        MAP_SIZE * (keySize + valueSize)
      }
      case _ => throw new Exception("Invalid primitive object inspector category")
    }
    return size
  }
}
