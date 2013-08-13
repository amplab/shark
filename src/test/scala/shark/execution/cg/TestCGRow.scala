package shark.execution.cg

import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.junit.Test
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.`lazy`.LazyStruct
import org.junit.Before
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import shark.execution.cg.row.CGRow
import shark.execution.cg.row.CGStruct
import org.fusesource.scalate.TemplateEngine
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import java.util.HashMap
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import java.util.ArrayList

class TestCGRow extends FunSuite with BeforeAndAfterEach {
  import collection.JavaConversions._
  
  private def createByteRef() = {
    var ref = new ByteArrayRef()
    var bytes = ("12,35,james,31,1234.6,2012-03-28 17:25:58,," + 
                 "1:aa|bb|cc,1|2|3,2013-02-16,1,2,3,4,5,6,7,8,CQ==,\0").getBytes()
    ref.setData(bytes)

    ref
  }

  private def createByteRef2() = {
    var ref = new ByteArrayRef()
    var bytes = "12,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0".getBytes()
    ref.setData(bytes)

    ref
  }

  private val fields = Array(
    "id", "depart", "name", "age", "salary",
    "jointime", "fake", "u", "l", "m", "birth",
    "testbool", "testbyte", "testshort", "testint",
    "testfloat", "testlong", "testdouble",
    "teststring", "testbinary", "testnullstring")
    
  private var s = TypeInfoFactory.getStructTypeInfo(
    List("a", "b", "c"),
    List(TypeInfoFactory.stringTypeInfo,
      TypeInfoFactory.stringTypeInfo,
      TypeInfoFactory.stringTypeInfo)
  )

  private var l = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.longTypeInfo)
  
  private var u = TypeInfoFactory.getUnionTypeInfo(
    List(TypeInfoFactory.longTypeInfo, s)
  )
  private var m = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.intTypeInfo)

  private val types = Array(TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.timestampTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    u,
    l,
    m,
    TypeInfoFactory.dateTypeInfo,
    TypeInfoFactory.booleanTypeInfo,
    TypeInfoFactory.byteTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.intTypeInfo,
    TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.doubleTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.binaryTypeInfo,
    TypeInfoFactory.stringTypeInfo
  )
      
  private def createOI() = {
    LazyFactory.createLazyStructInspector(
    fields.toList,
    types.toList,
    ",:|".getBytes(),
    new Text("\0"),
    false,
    false,
    0.asInstanceOf[Byte])
  }

  var oi : ObjectInspector = createOI()
  var cachedLazyStruct : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cachedLazyStruct1 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cachedLazyStruct2 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var ref = createByteRef()
  var ref1 = createByteRef()
  var ref2 = createByteRef2()
  
  var cgcachedLazyStruct : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cgcachedLazyStruct1 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cgcachedLazyStruct2 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cgref = createByteRef()
  var cgref1 = createByteRef()
  var cgref2 = createByteRef2()

  @Before
  override def beforeEach() {
    cachedLazyStruct.init(ref, 0, ref.getData().length)
    cachedLazyStruct1.init(ref1, 0, ref1.getData().length)
    cachedLazyStruct2.init(ref2, 0, ref2.getData().length)
    
    cgcachedLazyStruct.init(cgref, 0, cgref.getData().length)
    cgcachedLazyStruct1.init(cgref1, 0, cgref1.getData().length)
    cgcachedLazyStruct2.init(cgref2, 0, cgref2.getData().length)    
  }
  
  @Test
  def testRowComplicated() {
    println(CGRow.generate(oi.asInstanceOf[StructObjectInspector]))
  }
  
  @Test
  def testRowConstant() {
    var v = new HashMap[Text, java.util.List[IntWritable]]()
    var l1 = new ArrayList[IntWritable]()
    l1.add(new IntWritable(1))
    l1.add(new IntWritable(2))
    l1.add(new IntWritable(3))
    v.put(new Text("key1"), l1)
    
    var l2 = new ArrayList[IntWritable]()
    l2.add(new IntWritable(1))
    l2.add(new IntWritable(2))
    l2.add(new IntWritable(3))
    v.put(new Text("key2"), l2)
    
    var l3 = new ArrayList[Long]()
    l3.add(1l)
    l3.add(2l)
    l3.add(3l)
    
    var struct = ObjectInspectorFactory.getStandardStructObjectInspector(
        Array("test_long", "test_string", "test_union", "test_map", "test_list", "test_date", "test_timestamp").toList,
        Array(
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.longTypeInfo), 
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo),
            ObjectInspectorFactory.getStandardUnionObjectInspector(
                Array(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo),
                TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.longTypeInfo)).toList),
            ObjectInspectorFactory.getStandardConstantMapObjectInspector(
                TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo), 
                TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo)), 
                v),
            ObjectInspectorFactory.getStandardConstantListObjectInspector(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory.longTypeInfo), 
                l3),
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.dateTypeInfo),
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.timestampTypeInfo)
            ).toList
        )
    
    println(CGRow.generate(struct))
  }
}