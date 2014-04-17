/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution.cg

import java.sql.Timestamp
import shark.execution.cg.row.CGRow
import shark.execution.cg.row.CGOIStruct
import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGField
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import java.util.BitSet
import java.io.File
import shark.execution.cg.row.CGStruct
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.io.Writable
import io.netty.buffer.ByteBuf
import io.netty.buffer.SwappedByteBuf
import java.io.DataOutput
import java.nio.ByteBuffer
import java.io.DataInputStream
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.ql.exec.PTFPersistence.ByteBufferInputStream
import org.apache.hadoop.hive.ql.exec.PTFPersistence.ByteBufferOutputStream
import shark.execution.serialization.KryoSerializer
import org.apache.spark.SparkEnv
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.KryoSerializable
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory
import shark.execution.CGExecOperator
import shark.execution.CGOperator
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import java.util.Properties
import org.apache.hadoop.hive.serde.Constants
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import shark.execution.cg.row.CGOIField
import shark.execution.cg.row.CGOI

class TestPerformance extends FunSuite with BeforeAndAfterEach {
  private var oldCL: ClassLoader = _
  private var cc: CompilationContext = _

  override def beforeEach() {
    oldCL = Thread.currentThread().getContextClassLoader()
    cc = new CompilationContext()
    Thread.currentThread().setContextClassLoader(cc.preCompiledClassLoader)
  }

  def testCGSerDe(count: Int, data: KryoSerializable, op: OperatorExecutor) {
    import org.apache.spark.serializer.KryoSerializer

    val kryo = new Kryo()
    kryo.register(data.getClass())

    val buffer = new Array[Byte](102400)

    val output = new Output(buffer)
    val input = new Input(buffer)

    var i = count
    var o = data
    var t: AnyRef = null
    while (i > 0) {
      o.write(kryo, output)
      output.flush()
      input.setTotal(output.position())
      o.read(kryo, input)
      output.clear()
      input.rewind()

      t = op.evaluate(o)

      i -= 1
    }
  }

  def testCGSerDeOI(count: Int, data: KryoSerializable, soi: StructObjectInspector) {
    import org.apache.spark.serializer.KryoSerializer

    val kryo = new Kryo()
    kryo.register(data.getClass())

    val buffer = new Array[Byte](102400)

    val output = new Output(buffer)
    val input = new Input(buffer)

    var i = count
    var o = data
    var t: AnyRef = null
    while (i > 0) {
      o.write(kryo, output)
      output.flush()
      input.setTotal(output.position())
      o.read(kryo, input)
      output.clear()
      input.rewind()

      t = soi.getStructFieldsDataAsList(data)

      i -= 1
    }
  }

  def testHiveSerDe(count: Int, data: Array[Writable], soi: StructObjectInspector) = {
    var i = count

    val buffer = ByteBuffer.allocate(10240)
    val in = new ByteBufferInputStream(); in.intialize(buffer)
    val out = new ByteBufferOutputStream(); out.intialize(buffer)
    val input = new DataInputStream(in)
    val output = new DataOutputStream(out)

    val sfs = soi.getAllStructFieldRefs().toSeq

    val struct = new LazyBinarySerDe()
    val properties = new Properties()

    properties.setProperty("columns", 
      soi.getAllStructFieldRefs().map(r => r.getFieldName()).mkString(","))
    properties.setProperty("columns.types", soi.getAllStructFieldRefs().map(r => 
      TypeInfoUtils.getTypeInfoFromObjectInspector(r.getFieldObjectInspector())).mkString(","))

    struct.initialize(null, properties)
    var all: java.util.List[AnyRef] = null
    while (i > 0) {
      //      sfs.foreach(sf => {
      //        val w = soi.getStructFieldData(data, sf).asInstanceOf[Writable]
      //        output.writeBoolean(w != null)
      //        if(w != null) w.write(output)
      //      })
      //      output.flush()
      //      buffer.flip()
      //      var idx = 0
      //      while(idx < data.length) {
      //      	if(input.readBoolean()) data(idx).readFields(input)
      //        idx += 1
      //      }
      //      buffer.clear()

      val writable = struct.serialize(data, soi)
      val o = struct.deserialize(writable)
      val oi = struct.getObjectInspector().asInstanceOf[StructObjectInspector]
      all = oi.getStructFieldsDataAsList(o)
      i = i - 1
    }
  }

  def testCGExpr(count: Int, obj: AnyRef, executor: OperatorExecutor) {
    var i = count

    while (i > 0) {
      val a = executor.evaluate(obj)
      i = i - 1
    }
  }

  def testHiveExpr(count: Int, obj: AnyRef, executor: ExprNodeEvaluator) {
    var i = count

    while (i > 0) {
      val a = executor.evaluate(obj)
      i = i - 1
    }
  }

  def time(desc: String, func: => Unit) {
    val s = System.currentTimeMillis()
    func
    val e = System.currentTimeMillis()

    println(s"$desc takes ${e - s} ms")
  }

  import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
  import scala.collection.JavaConversions._
  def createOI: StructObjectInspector = {
    ObjectInspectorFactory.getStandardStructObjectInspector(
      seqAsJavaList("f_short" ::
        "f_int" ::
        "f_long" ::
        "f_double" ::
        "f_string" ::
        "f_float" ::
        "f_boolean" :: Nil),
      seqAsJavaList(POIF.writableShortObjectInspector ::
        POIF.writableIntObjectInspector ::
        POIF.writableLongObjectInspector ::
        POIF.writableDoubleObjectInspector ::
        POIF.writableStringObjectInspector ::
        POIF.writableFloatObjectInspector ::
        POIF.writableBooleanObjectInspector ::
        Nil))
  }

  def createOutputOI: StructObjectInspector = {
    ObjectInspectorFactory.getStandardStructObjectInspector(
      seqAsJavaList("f_double" :: Nil),
      seqAsJavaList(POIF.writableDoubleObjectInspector :: Nil))
  }

  def exec(execClassName: String, cgrow: CGStruct): (String, String) = {
    (execClassName, s"""
        package ${execClassName.substring(0, execClassName.lastIndexOf('.'))};

        import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
        import shark.execution.cg.OperatorExecutor;

        public class ${execClassName.substring(execClassName.lastIndexOf('.') + 1, 
            execClassName.length())} extends OperatorExecutor {
          @Override
          public Object evaluate(Object o) throws Exception {
            ${cgrow.dynamicFullClassName} row = (${cgrow.dynamicFullClassName})o;
            ${cgrow.dynamicFullClassName} output = new ${cgrow.dynamicFullClassName}();
            if(row.mask.get(${cgrow.getMaskBitVariableName(0)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(0)}, true);
//               output.${cgrow.fields(0).name} = row.${cgrow.fields(0).name};
            }
            if(row.mask.get(${cgrow.getMaskBitVariableName(1)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(1)}, true);
//               output.${cgrow.fields(1).name} = row.${cgrow.fields(1).name};
            }

               if(row.mask.get(${cgrow.getMaskBitVariableName(2)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(2)}, true);
//               output.${cgrow.fields(2).name} = row.${cgrow.fields(2).name};
            }

               if(row.mask.get(${cgrow.getMaskBitVariableName(3)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(3)}, true);
//               output.${cgrow.fields(3).name} = row.${cgrow.fields(3).name};
            }
               if(row.mask.get(${cgrow.getMaskBitVariableName(4)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(4)}, true);
//               output.${cgrow.fields(4).name} = row.${cgrow.fields(4).name};
            }
               if(row.mask.get(${cgrow.getMaskBitVariableName(5)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(5)}, true);
//               output.${cgrow.fields(5).name} = row.${cgrow.fields(5).name};
            }
            if(row.mask.get(${cgrow.getMaskBitVariableName(6)})) {
//               output.mask.set(${cgrow.getMaskBitVariableName(6)}, true);
//               output.${cgrow.fields(6).name} = row.${cgrow.fields(6).name};
            }
               output.mask = row.mask;
               return output;
          }
        }
        """)
  }

  def createDesc1 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_short,
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_int,
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_long,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_double,
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_float,
              f_float)))))
    ("short + (int + (long + (double + (float + float))))", desc)
  }

  def createDesc2 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_short, f_int)
    ("short + int", desc)
  }

  def createDesc3 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_short, f_int),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_short, f_int))

    ("(short + int) + (short + int)", desc)
  }

  def createDesc4 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or", f_boolean,
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("not", f_boolean)),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("not", f_boolean))

    ("(boolean || !boolean) && (!boolean)", desc)
  }

  def createDesc5 = {
    val desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("pow", f_short,
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("pow", f_double, f_float))

    ("pow(double, pow(double, float))", desc)
  }

  def createDesc6 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("hour", f_string)

    ("hour(string)", desc)
  }
  def createDesc7 = {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f_int, f_int)
    ("int+int", desc)
  }

  val c_str = new ExprNodeConstantDesc("2009-07-30 12:58:59")
  val f_short = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "f_short", "a", false)
  val f_int = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "f_int", "a", false)
  val f_long = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "f_long", "a", false)
  val f_double = new ExprNodeColumnDesc(TypeInfoFactory.doubleTypeInfo, "f_double", "a", false)
  val f_string = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "f_string", "a", false)
  val f_float = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "f_float", "a", false)
  val f_boolean = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "f_boolean", "a", false)

  val hiveObj = Array[Writable](new ShortWritable(0),
    new IntWritable(1),
    new LongWritable(2),
    new DoubleWritable(3.1),
    new Text("2009-07-30 12:58:59"),
    new FloatWritable(5),
    new BooleanWritable(true))

  test("primitive") {

    val soi = createOI
    val cgrow = CGField.create(soi)

    val descs = createDesc1 :: createDesc2 :: createDesc3 :: createDesc4 :: 
      createDesc5 :: createDesc6 :: createDesc7 :: Nil
    val outputOI = createOutputOI
    val outputRow = CGField.create(outputOI)
    val cgRowOI = CGOIField.create(cgrow).asInstanceOf[CGOIStruct]

    val hiveEvals = descs.map(d => (d._1, ExprNodeEvaluatorFactory.get(d._2)))
    val cgOperators = descs.map(d => (d._1, CGExecOperator(Seq("v1"), Seq(d._2), cgrow, outputRow)))

    val cgrowAccessorClassName = "shark.execution.cg.row.Accessor"

    time("compiling:", cc.compile(
      (cgrow.fullClassName, CGRow.generate(cgrow, true)) ::
        (cgRowOI.fullClassName, CGOI.generateOI(cgRowOI, true)) ::
        exec(cgrowAccessorClassName, cgrow) ::
        cgOperators.map(e => (e._2.fullClassName, CGOperator.generate(e._2)))))

    val cgEvals = cgOperators.map(d => (d._1, 
      BeanPropertyHelper.instantiate(d._2.fullClassName).asInstanceOf[OperatorExecutor]))

    val cgrowAccessor = BeanPropertyHelper.instantiate(cgrowAccessorClassName).asInstanceOf[OperatorExecutor]
    val cgObj = BeanPropertyHelper.instantiate(cgrow.dynamicFullClassName).asInstanceOf[KryoSerializable]
    val cgOi = BeanPropertyHelper.instantiate(cgRowOI.fullClassName).asInstanceOf[StructObjectInspector]

    val mask = BeanPropertyHelper.getPropertyValue(cgObj, 
      CGField.STRUCT_MASK_VARIABLE_NAME).asInstanceOf[BitSet]
    (0 to cgrow.fields.length) foreach { i => mask.set(i, true) }
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(0).name, new java.lang.Short(0.asInstanceOf[Short]))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(1).name, new java.lang.Integer(1))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(2).name, new java.lang.Long(2))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(3).name, new java.lang.Double(3.1))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(4).name, new java.lang.String("aaabbb"))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(5).name, new java.lang.Float(5))
    BeanPropertyHelper.setPropertyValue(cgObj, cgrow.fields(6).name, new java.lang.Boolean(true))

    val count = 10000000

    time("hive serde", testHiveSerDe(count, hiveObj, soi))
    time("cg_serde", testCGSerDe(count, cgObj, cgrowAccessor))
    time("cg_serde_oi", testCGSerDeOI(count, cgObj, cgOi))

    hiveEvals.foreach(e => e._2.initialize(soi))
    cgEvals.foreach(e => e._2.init(Array(soi)))

    hiveEvals.foreach(e => time(s"Hive: ${e._1}", testHiveExpr(count, hiveObj, e._2)))
    cgEvals.foreach(e => time(s"CG: ${e._1}", testCGExpr(count, hiveObj, e._2)))
  }
}