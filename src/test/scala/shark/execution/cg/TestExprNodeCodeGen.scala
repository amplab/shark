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

import java.io.File
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.DataOutputStream
import java.io.ObjectOutputStream
import java.io.DataInputStream
import java.io.ObjectInputStream
import java.io.IOException
import java.sql.Timestamp
import java.sql.Date
import java.util.Arrays

import org.apache.hadoop.hive.contrib.udf.UDFRowSequence
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.udf.UDFLog2
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.LazyStruct
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory

import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.junit.Test
import org.junit.Before

import shark.execution.cg.udf.UDFStateful



class TestExprNodeCodeGen extends FunSuite with BeforeAndAfterEach {
  import collection.JavaConversions._
  
  // TODO work around solution for sbt unit test
  System.setProperty(JavaCompilerHelper.FOR_UNIT_TEST_WORK_AROUND, "true")
  
  private def createByteRef() = {
    var ref = new ByteArrayRef()
    var bytes = ("12,35,james,31,1234.6,2012-03-28 17:25:58,," + 
                 "1:aa|bb|cc,2013-02-16,1,2,3,4,5,6,7,8,CQ==,\0").getBytes()
    ref.setData(bytes)

    ref
  }

  private def createByteRef2() = {
    var ref = new ByteArrayRef()
    var bytes = "12,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0,\0".getBytes()
    ref.setData(bytes)

    ref
  }

  private val fields = Array(
    "id", "depart", "name", "age", "salary",
    "jointime", "fake", "u", "birth",
    "testbool", "testbyte", "testshort", "testint",
    "testfloat", "testlong", "testdouble",
    "teststring", "testbinary", "testnullstring")
    
  private var s = TypeInfoFactory.getStructTypeInfo(
    List("a", "b", "c"),
    List(TypeInfoFactory.stringTypeInfo,
      TypeInfoFactory.stringTypeInfo,
      TypeInfoFactory.stringTypeInfo)
  )

  private var u = TypeInfoFactory.getUnionTypeInfo(
    List(TypeInfoFactory.longTypeInfo, s)
  )

  private val types = Array(TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.timestampTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    u,
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

  test("unary operator") {
    // id isnotnull
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull", name)

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("constant primitives") {
    // 111
    var desc = new ExprNodeConstantDesc(111.asInstanceOf[Short])

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("algebraic +") {
    // 40000(short) + 40000(short) 
    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
          new ExprNodeConstantDesc(40000.asInstanceOf[Short]),
          new ExprNodeConstantDesc(40000.asInstanceOf[Short]))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("algebraic -") {
    // 4000 - 4000
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("-",
          new ExprNodeConstantDesc(4000.asInstanceOf[Short]),
          new ExprNodeConstantDesc(4000.asInstanceOf[Short]))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("udf & constant variable and constant variable") {
    // concat('abc_','cde')
    var desc = 
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
        new ExprNodeConstantDesc("abc_"),
        new ExprNodeConstantDesc("cde"))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("udf & constant variable + field value") {
    // concat('abc_',name)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
          new ExprNodeConstantDesc("abc_"),
          name)

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("udf & field variable + field variable") {
    // concat(substr(name,0,1), substr(name,0,1))
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            name,
            new ExprNodeConstantDesc(0),
            new ExprNodeConstantDesc(1)),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            name,
            new ExprNodeConstantDesc(0),
            new ExprNodeConstantDesc(1)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("udf & null value") {
    // concat(substr(fake, 0, 1), substr(name,0,1))
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
          // TODO to make a void data
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null) //fake
            , new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(1)),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            name,
            new ExprNodeConstantDesc(0),
            new ExprNodeConstantDesc(1)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("logical expression & udf") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    //  ((id>5 && id <15) && (substr(name, 0,4)>="james" && substr(name,0,4)<="qames")
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
              id,
              new ExprNodeConstantDesc(5)),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
              id,
              new ExprNodeConstantDesc(15))),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">=",
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                name,
                new ExprNodeConstantDesc(0),
                new ExprNodeConstantDesc(4)),
              new ExprNodeConstantDesc("iames")),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                name,
                new ExprNodeConstantDesc(0),
                new ExprNodeConstantDesc(4)),
              new ExprNodeConstantDesc("qames"))))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("logical operator") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    // (age = 31) (id = 12)
    //   ((age>25) || (id != 3)) && ((id !=3) && ((age>25 && age<=35)))

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
          age,
          new ExprNodeConstantDesc(25)),
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
          id,
          new ExprNodeConstantDesc(3))),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
          id,
          new ExprNodeConstantDesc(3)),
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
            age,
            new ExprNodeConstantDesc(25)),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",
            age,
            new ExprNodeConstantDesc(35)))))

                
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("binary operator") {
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%",
          age,
          new ExprNodeConstantDesc(10))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("complicated expression eval (string funcs)") {
    // concat(substr(depart,0,age%10), substr(name,0,1))
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            depart,
            new ExprNodeConstantDesc(0),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%",
              age,
              new ExprNodeConstantDesc(10))),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
            name,
            new ExprNodeConstantDesc(0),
            new ExprNodeConstantDesc(1)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("op or || 1") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age),
      new ExprNodeConstantDesc(true)
    )
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("op or || 2") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
      new ExprNodeConstantDesc(true),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age)
    )
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
    
  test("op or || 3") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, null),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age)
    ) 
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("op or || 4") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age),
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, null)
    ) 
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
    
  test("EqualNS 1") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age),
      new ExprNodeConstantDesc(true)
    )
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("EqualNS 2") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>",
      new ExprNodeConstantDesc(true),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age)
    )
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
    
  test("EqualNS 3") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>",
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, null),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        age)
    ) 
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("complicated expression eval (logical operators & string funcs)") {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    // (((id<=-100 or id>=0) and (id>0 and age != 31))
    //       or
    //  ((age<=>id and not(age=id)) and (concat("abc_",substr(name,0,3%age)) > "abc_") 
    //    and 
    //  name>depart)
    // )
    // and (age is not null and salary>age*10)
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",
                  id,
                  new ExprNodeConstantDesc(-100)),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">=",
                  id,
                  new ExprNodeConstantDesc(0))),
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                  id,
                  new ExprNodeConstantDesc(0)),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                  age,
                  new ExprNodeConstantDesc(31)))),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>",
                    age,
                    id),
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age, id))),

                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                    new ExprNodeConstantDesc("abc_"),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                      name,
                      new ExprNodeConstantDesc(0),
                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%",
                        new ExprNodeConstantDesc(3),
                        age))),
                  new ExprNodeConstantDesc("abc_"))),
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", name, depart))),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull", age),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
              salary,
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                age,
                new ExprNodeConstantDesc(10)))))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("stateful function1") {
    // row_sequence() * row_sequence() // should be row1: 1*1  row2: 2*2 row3: 3*3
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)

    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("stateful function2") {
    // row_sequence() * 1 + row_sequence() * 1
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)
    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
            new ExprNodeConstantDesc(1)),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
            new ExprNodeConstantDesc(1)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("parametered stateful UDF") {
    FunctionRegistry.registerTemporaryUDF("udf_stateful", classOf[UDFStateful], false)
    
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
                "udf_stateful", id),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
                "udf_stateful", id)
        )

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)

    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
    
  test("parametered stateful UDF in paritial evaluating") {
    FunctionRegistry.registerTemporaryUDF("udf_stateful", classOf[UDFStateful], false)
    
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    
    var desc = 
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
          new ExprNodeConstantDesc("2"),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("udf_stateful", id)
        ),
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=",
          new ExprNodeConstantDesc("2"),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("udf_stateful", id)
        )
      )

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)

    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("deterministic-less function") {
    // rand() * rand() == rand() * rand()
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("==",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand")),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand")))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)

    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("double null-value checking for id") {
    // id>1 and id<4
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
            id,
            new ExprNodeConstantDesc(1)),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
            id,
            new ExprNodeConstantDesc(4)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("double null-value checking for id in complicated case") {
    // (id>1 or id>5) or (id !=6)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
              id,
              new ExprNodeConstantDesc(1)),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
              id,
              new ExprNodeConstantDesc(5))),
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
            id,
            new ExprNodeConstantDesc(6)))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("concat") {
    // concat(id,'_',depart,'_',name,'_',age,'_',salary,'_',jointime,'_',fake)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var fake = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "fake", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,

    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
          id,
          new ExprNodeConstantDesc("_"),
          depart,
          new ExprNodeConstantDesc("_"),
          name,
          new ExprNodeConstantDesc("_"),
          age,
          new ExprNodeConstantDesc("_"),
          salary,
          new ExprNodeConstantDesc("_"),
          jointime,
          new ExprNodeConstantDesc("_"),
          fake)

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("between") {
    // age between 30 and 32
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,

    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("between",
          new ExprNodeConstantDesc(false),
          age,
          new ExprNodeConstantDesc(30),
          new ExprNodeConstantDesc(32))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("printf") {
    // printf("%d_%s_%s_%d_%.1f_%tF_%s",id,depart,name,age,salary,joitime,fake)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var fake = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "fake", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var format = "%d_%s_%s_%d_%.1f_%tF_%s"
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("printf",
          new ExprNodeConstantDesc(format),
          id,
          depart,
          name,
          age,
          salary,
          jointime,
          fake)

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    // keep throwing the same exception with HIVE GenericUDFPrintf
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("instr") {
    // instr(name,'me')
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
          name,
          new ExprNodeConstantDesc("me"))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("instr2") {
    // instr(name,depart)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
          name,
          depart)

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
    
  test("int can be converted into string") {
    // instr(name,3), test if the int can be converted into string
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc = 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
          name,
          new ExprNodeConstantDesc(3))
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("instr + printf") {
    //instr(printf("%d_%s_%s_%d_%.1f_%tF_%s",id,depart,name,age,salary,joitime,fake), name)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var fake = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "fake", "a", false)

    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var format = "%d_%s_%s_%d_%.1f_%tF_%s"
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("printf",
        new ExprNodeConstantDesc(format),
        id,
        depart,
        name,
        age,
        salary,
        jointime,
        fake),
      name)
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("short can not be converted into string") {
    // name > age, test if the age (short) can be converted into string
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        name,
        age)
        
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)

    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("short can be converted into string") {
    // row_sequence()>1 and name > age , test if the age (short) can be converted into string
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
        new ExprNodeConstantDesc(1)),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        name,
        age))
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("null value evaluating in code gen") {
    // name > age, test if the age (short) can be converted into string
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    // name>id
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
      name,
      age)
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("assert exception raises 1") {
    // map(name,id)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    //as Map is not supported yet, it will raise the exception.
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("map",
      name,
      age)

    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    intercept[CGAssertRuntimeException] {
      cgeval.initialize(oi)
    }
  }
    
  test("common sub expression 1") {
    //concat(year(date_add(jointime,7),'/',month(date_add(jointime,7),'/',day(date_add(jointime,7)))
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("year", 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("date_add", 
          jointime, 
          new ExprNodeConstantDesc(7))),
      new ExprNodeConstantDesc("/"),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("month", 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("date_add", 
          jointime, 
          new ExprNodeConstantDesc(7))),
      new ExprNodeConstantDesc("/"),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("day", 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("date_add", 
          jointime, new ExprNodeConstantDesc(7)))
    )
    
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("common sub expression 2") {
    // (id>0) && (id>0 || age>0) 
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
        id,
        new ExprNodeConstantDesc(0)),
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or", 
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", 
          id,
          new ExprNodeConstantDesc(0)),
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", 
          age, 
          new ExprNodeConstantDesc(0))
      )
    )
    
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("union") {
    var b = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "u:1.b", "a", false)
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", 
          b,
          new ExprNodeConstantDesc("bb"))
          
    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(new BooleanWritable(true) == cgeval.evaluate(cgcachedLazyStruct))
    assert(new BooleanWritable(true) == cgeval.evaluate(cgcachedLazyStruct1))
    assert(null == cgeval.evaluate(cgcachedLazyStruct2))
    // TODO seems there is bug in union data parsing in Hive Evaluator
//    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
//    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
//    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("code gen manager") {
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", 
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "u:1.b", "a", false),
      new ExprNodeConstantDesc("bb")).asInstanceOf[ExprNodeGenericFuncDesc]
      
    var raw = CGClassEntry(desc)
    
    for (i <- 1 until 20) {
      var cgm = CGClassEntry(desc.clone().asInstanceOf[ExprNodeGenericFuncDesc])
      assert(cgm.eq(raw))
    }
  }

  test("null of root expression") {
    // birth:02-16-2013
    // birth<"03-05-2013" 
    var birth = new ExprNodeColumnDesc(TypeInfoFactory.dateTypeInfo, "birth", "a", false)

    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
        birth,
        new ExprNodeConstantDesc("03-05-2013"))

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }
  
  test("null of non root expression") {
    // birth:02-16-2013
    // birth<"03-05-2013" 
    var birth = new ExprNodeColumnDesc(TypeInfoFactory.dateTypeInfo, "birth", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var desc = 
      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
          birth,
          new ExprNodeConstantDesc("03-05-2013")),
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
          name,
          new ExprNodeConstantDesc(0))
       )

    var eval = ExprNodeEvaluatorFactory.get(desc)
    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
    
    eval.initialize(oi)
    cgeval.initialize(oi)
    
    assert(eval.evaluate(cachedLazyStruct)== cgeval.evaluate(cgcachedLazyStruct))
    assert(eval.evaluate(cachedLazyStruct1)== cgeval.evaluate(cgcachedLazyStruct1))
    assert(eval.evaluate(cachedLazyStruct2)== cgeval.evaluate(cgcachedLazyStruct2))
  }

  test("Unary UDF Code Gen") {
    var cgResults = ArrayBuffer[String]()
    var hiveResults = ArrayBuffer[String]()
    
    var descs = ArrayBuffer[(String, ExprNodeDesc)]()
    var ops = Array("+", "-", "!", "isnotnull", "isnull", "~")
    for (a <- 0 until ops.length) 
    for (b <- 0 until types.length) {
      if ("u" != fields(b)) { 
        var op = ops(a)
        var x = new ExprNodeColumnDesc(types(b), fields(b), "a", false)
        
        beforeEach()
        
        var desc: ExprNodeDesc = null
        
        try {
          desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(op,x)
        } catch {case _ => /* do nothing */ } 
  
        if (desc != null) {
          var eval = ExprNodeEvaluatorFactory.get(desc)
          var cgeval = CGEvaluatorFactory.getEvaluator(desc)

          hiveResults += (testConvert(op + " " + fields(b), eval, cachedLazyStruct))
          cgResults += (testConvert(op + " " + fields(b), cgeval, cachedLazyStruct))
          
          hiveResults += (testConvert(op + " " + fields(b), eval, cachedLazyStruct1))
          cgResults += (testConvert(op + " " + fields(b), cgeval, cachedLazyStruct1))
          
          hiveResults += (testConvert(op + " " + fields(b), eval, cachedLazyStruct2))
          cgResults += (testConvert(op + " " + fields(b), cgeval, cachedLazyStruct2))
        }
      }
    }
    
    compareResult(hiveResults, cgResults, true)
  }

  test("constant nullvariable") {
    var cgResults = ArrayBuffer[String]()
    var hiveResults = ArrayBuffer[String]()

    var bytes = new ByteArrayRef()
    bytes.setData(Array(9.asInstanceOf[Byte]))
    
    var descs = Array(
      new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, bytes),
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true),
      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 1),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 2l),
      new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "3"),
      new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, 4f),
      new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5d),
      new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, 6.asInstanceOf[Byte]),
      new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, 7.asInstanceOf[Short]),
      new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, new Date(9)),
      new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, new Timestamp(10)),
      
      new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, null),
      new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, null)
    )

    for (a <- 0 until descs.length)
      for (b <- 0 until descs.length) {
        var desc: ExprNodeDesc = null
        var d1 = descs(a)
        var d2 = descs(b)
        try {
          desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and", 
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull", d1), 
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnull", d2))
        } catch {
          case _ => println (d1 + "<---->" + d2)
        } 

        if (desc != null) {
          var eval = ExprNodeEvaluatorFactory.get(desc)
          var cgeval = CGEvaluatorFactory.getEvaluator(desc)
  
          hiveResults += (testConvert(d1 + " + " + d2, eval, cachedLazyStruct))
          cgResults += (testConvert(d1 + " + " + d2, cgeval, cgcachedLazyStruct))
        }
      }

    compareResult(hiveResults, cgResults, true)
  }
  
  test("constant variable") {
    var cgResults = ArrayBuffer[String]()
    var hiveResults = ArrayBuffer[String]()

    var bytes = new ByteArrayRef()
    bytes.setData(Array(9.asInstanceOf[Byte]))
    
    var descs = Array(
      new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, bytes),
      new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true),
      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 1),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 2l),
      new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "3"),
      new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, 4f),
      new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5d),
      new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, 6.asInstanceOf[Byte]),
      new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, 7.asInstanceOf[Short]),
      new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, new Date(9)),
      new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, new Timestamp(10))
    )

    for (a <- 0 until descs.length)
      for (b <- 0 until descs.length) {
        var desc: ExprNodeDesc = null
        var d1 = descs(a)
        var d2 = descs(b)
        try {
          desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", d1, d2)
        } catch {
          case _ => println (d1 + "+" + d2)
        } 

        if (desc != null) {
          var eval = ExprNodeEvaluatorFactory.get(desc)
          var cgeval = CGEvaluatorFactory.getEvaluator(desc)
  
          hiveResults += (testConvert(d1 + " + " + d2, eval, cachedLazyStruct))
          cgResults += (testConvert(d1 + " + " + d2, cgeval, cgcachedLazyStruct))
        }
      }

    compareResult(hiveResults, cgResults, true)
  }
  
  test("udf unary UDF") {
    var cgResults = ArrayBuffer[String]()
    var hiveResults = ArrayBuffer[String]()
    FunctionRegistry.registerTemporaryUDF("log", classOf[UDFLog2], false)

    var descs = ArrayBuffer[(String, ExprNodeDesc)]()
    
    for (a <- 0 until fields.length)
      for (b <- 0 until types.length) {
        beforeEach()
        var x = new ExprNodeColumnDesc(types(a), fields(a), "a", false)
        var y = new ExprNodeColumnDesc(types(b), fields(b), "a", false)
        
        var desc: ExprNodeDesc = null
        
        try {
          desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("log", x),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("log", y))
        } catch {case _ => /* do nothing */ } 

        if (desc != null) {
          var eval = ExprNodeEvaluatorFactory.get(desc)
          var cgeval = CGEvaluatorFactory.getEvaluator(desc)
          
          hiveResults += (testConvert(fields(a) + "->" + fields(b), eval, cachedLazyStruct))
          cgResults += (testConvert(fields(a) + "->" + fields(b), cgeval, cachedLazyStruct))
          
          hiveResults += (testConvert(fields(a) + "->" + fields(b), eval, cachedLazyStruct1))
          cgResults += (testConvert(fields(a) + "->" + fields(b), cgeval, cachedLazyStruct1))
          
          hiveResults += (testConvert(fields(a) + "->" + fields(b), eval, cachedLazyStruct2))
          cgResults += (testConvert(fields(a) + "->" + fields(b), cgeval, cachedLazyStruct2))
        }
      }
    
    compareResult(hiveResults, cgResults, true)
  }

  test("if") {
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var d = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 3.629.asInstanceOf[Double])
    
    var v1 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", age, id)
    var v2 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("-", age, id)
    
    var descs = ArrayBuffer[ExprNodeDesc]()
    
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "if", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age, age),
          v1,
          v2)
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "if", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age, id),
          v1,
          v2)
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "if", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", d, id),
          v1,
          v2)
    
    descs.foreach(desc=> {
      var cgeval = CGEvaluatorFactory.getEvaluator(desc)
      var hiveeval = ExprNodeEvaluatorFactory.get(desc)
      cgeval.initialize(oi)
      hiveeval.initialize(oi)

//      println(hiveeval.evaluate(cachedLazyStruct))
//      println(cgeval.evaluate(cgcachedLazyStruct))
//      println(hiveeval.evaluate(cachedLazyStruct1))
//      println(cgeval.evaluate(cgcachedLazyStruct1))
//      println(hiveeval.evaluate(cachedLazyStruct2))
//      println(cgeval.evaluate(cgcachedLazyStruct2))

      assert(cgeval.evaluate(cgcachedLazyStruct) == hiveeval.evaluate(cachedLazyStruct))
      assert(cgeval.evaluate(cgcachedLazyStruct1) == hiveeval.evaluate(cachedLazyStruct1))
      assert(cgeval.evaluate(cgcachedLazyStruct2) == hiveeval.evaluate(cachedLazyStruct2))
    })
  }
  
  test("when") {
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var d = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 3.629.asInstanceOf[Double])
    
//    var salary_d = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", salary, d)
//    var age_d = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", age, d)
//    var id_d = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", id, d)
    var salary_d = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 1.629.asInstanceOf[Double])
    var age_d = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 2.629.asInstanceOf[Double])
    var id_d = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 3.629.asInstanceOf[Double])
    
    var descs = ArrayBuffer[ExprNodeDesc]()
    
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "when", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age_d, age_d),
          salary_d,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age_d, salary),
          age_d,
          id_d)
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "when", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age_d, id_d),
          salary_d,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age, age),
          age_d,
          id_d)
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "when", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age_d, id_d),
          salary_d,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=", age_d, id),
          age_d,
          id_d)
    
    descs.foreach(desc=> {
      var cgeval = CGEvaluatorFactory.getEvaluator(desc)
      var hiveeval = ExprNodeEvaluatorFactory.get(desc)
      cgeval.initialize(oi)
      hiveeval.initialize(oi)

//      println(hiveeval.evaluate(cachedLazyStruct))
//      println(cgeval.evaluate(cgcachedLazyStruct))
//      println(hiveeval.evaluate(cachedLazyStruct1))
//      println(cgeval.evaluate(cgcachedLazyStruct1))
//      println(hiveeval.evaluate(cachedLazyStruct2))
//      println(cgeval.evaluate(cgcachedLazyStruct2))

      assert(cgeval.evaluate(cgcachedLazyStruct) == hiveeval.evaluate(cachedLazyStruct))
      assert(cgeval.evaluate(cgcachedLazyStruct1) == hiveeval.evaluate(cachedLazyStruct1))
      assert(cgeval.evaluate(cgcachedLazyStruct2) == hiveeval.evaluate(cachedLazyStruct2))
    })
  }
  
  test("expression case") {
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var v1 = new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, 1.asInstanceOf[Short])
    var v2 = new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, 2.asInstanceOf[Short])
    var v3 = new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, 3.asInstanceOf[Short])
    
    var descs = ArrayBuffer[ExprNodeDesc]()
    
    // CASE age WHEN 1 THEN 2 END
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "case", age, // case age 
          v1, // when age = 1
          v2) // then 2
    // CASE age WHEN (1 + age) THEN (salary + 2) ELSE (salary+3) END
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "case", age, // case age 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", // when 
              v1,
              age), 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", // then 
              salary, 
              v2), 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", // else 
              salary, 
              v3)) 
    // CASE (age+1) WHEN (age+2) THEN (id + id) WHEN (age+3) THEN (id+age) ELSE (2+id) END
    descs += TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
          "case", 
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", age, v1), // case
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", age, v2), // when
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", id, id), // then
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", age, v3), // when
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", id, age), // then
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", v2, id)) // else
    
    descs.foreach(desc=> {
      var cgeval = CGEvaluatorFactory.getEvaluator(desc)
      var hiveeval = ExprNodeEvaluatorFactory.get(desc)
      cgeval.initialize(oi)
      hiveeval.initialize(oi)
      
      assert(cgeval.evaluate(cgcachedLazyStruct) == hiveeval.evaluate(cachedLazyStruct))
      assert(cgeval.evaluate(cgcachedLazyStruct1) == hiveeval.evaluate(cachedLazyStruct1))
      assert(cgeval.evaluate(cgcachedLazyStruct2) == hiveeval.evaluate(cachedLazyStruct2))
    })
  }
  
//  @Test
//  def test() {
//    var x = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "testnullstring", "a", false)
//    var y = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "testnullstring", "a", false)
//    var desc: ExprNodeDesc = null
//
//    try {
//      desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>", x, y)
//    } catch {
//      case _ => // do nothing
//    }
//
//    var exception_cg = false
//    var result_cg: AnyRef = null
//
//    var cgeval = CGEvaluatorFactory.getEvaluator(desc)
//    cgeval.initialize(oi)
//
//    try {
//      result_cg = cgeval.evaluate(cgcachedLazyStruct)
//      println(result_cg)
//    } catch { case ioe: Throwable => exception_cg = true }
//  }
  
  test("converter") {
    var cgResults = ArrayBuffer[String]()
    var hiveResults = ArrayBuffer[String]()
    
    var descs = ArrayBuffer[(String, ExprNodeDesc)]()
    var ops = Array("+", "-", "*", "/", "%", "pmod", "&", "|", "^", "~", 
                    "&&", "||", "=", "<=>", "<", "<=", ">", ">=")
    for (a <- 0 until fields.length)
      for (b <- 0 until types.length)
        for (c <- 0 until ops.length) {
          beforeEach()
          var x = new ExprNodeColumnDesc(types(a), fields(a), "a", false)
          var y = new ExprNodeColumnDesc(types(b), fields(b), "a", false)
          var op = ops(c)
          var desc: ExprNodeDesc = null

          try {
            desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(op, x, y)
          } catch {
            case _ => // do nothing
          }

          if (desc != null) {
            descs+=((fields(a) + " " + op + " " + fields(b), desc))
          }
        }
    
    descs.foreach(t => {
      beforeEach()
      
      var cgeval = CGEvaluatorFactory.getEvaluator(t._2)
      cgResults+=(testConvert(t._1, cgeval, cgcachedLazyStruct))
      
      var eval = ExprNodeEvaluatorFactory.get(t._2)
      hiveResults+=(testConvert(t._1, eval, cachedLazyStruct))
    })
    
    assert(hiveResults.length == cgResults.length)
    //TODO some behaviors are different between Hive / CG evaluator
    // (Raise RuntimeException V.S. Null Value in expression evaluating)
    compareResult(hiveResults, cgResults)
  }
  
  def compareResult(hiveResults: ArrayBuffer[String], 
      cgResults: ArrayBuffer[String], 
      stop: Boolean = false) {
    var compare = hiveResults.zip(cgResults).map(x=>{
        if(x._1 != x._2) {
          println(x._1 + "   " + x._2) 
          false
        } else 
          true
      }).foldLeft(true)(_ && _)
      
    if (stop) assert(compare)
  }

  def testConvert(expr: String, eval: ExprNodeEvaluator, cachedLazyStruct: LazyStruct) = {
    var result: AnyRef = null
    var exception = false
    eval.initialize(oi)
    try {
      result = eval.evaluate(cachedLazyStruct)
    } catch { case ioe: Throwable => exception = true }
    expr + " Result=>" + result + " Exception:" + exception
  }
}
