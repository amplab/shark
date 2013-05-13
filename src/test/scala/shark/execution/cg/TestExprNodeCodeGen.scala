package shark.execution.cg

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import java.sql.Timestamp
import java.util.Arrays
import java.util.Date
import org.apache.hadoop.hive.contrib.udf.UDFRowSequence
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
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
import org.junit.Before
import org.junit.Assert._
import org.junit.Test
//import org.apache.hadoop.hive.ql.exec.bytecode.example.AB

class TestExprNodeCodeGen {
  import collection.JavaConversions._
  private def createByteRef() = {
    var ref = new ByteArrayRef()

    var baos = new ByteArrayOutputStream()
    var dos = new DataOutputStream(baos)
    // to illustrate the table with id(long), depart(string), name(string), age(short), salary(float), jointime(Timestamp), fake(string)
    dos.write("12,sales,james,31,1234.6,2012-03-28 17:25:58,".getBytes())
    dos.flush()

    ref.setData(baos.toByteArray())
    
    ref
  }
  
  private def createByteRef2() = {
    var ref = new ByteArrayRef()

    var baos = new ByteArrayOutputStream()
    var dos = new DataOutputStream(baos)
    // to illustrate the table with id(long), depart(string), name(string), age(short), salary(float), jointime(Timestamp), fake(string)
    dos.write("12,\0,\0,\0,\0,\0,\0".getBytes())
    dos.flush()

    ref.setData(baos.toByteArray())
    
    ref
  }

  private def createOI() = LazyFactory.createLazyStructInspector(
        List("id", "depart", "name", "age", "salary", "jointime","fake"),
        List(
            TypeInfoFactory.longTypeInfo,
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.shortTypeInfo,
            TypeInfoFactory.floatTypeInfo,
            TypeInfoFactory.timestampTypeInfo,
            TypeInfoFactory.stringTypeInfo
        ),
        ",".getBytes(),
        new Text("\0"),
        false,
        false,
        0)

  var oi : ObjectInspector = createOI()
  var cachedLazyStruct : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cachedLazyStruct1 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var cachedLazyStruct2 : LazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  var ref = createByteRef()
  var ref1 = createByteRef()
  var ref2 = createByteRef2()
  
  @Before
  def setup() {
    cachedLazyStruct.init(ref, 0, ref.getData().length)
    cachedLazyStruct1.init(ref1, 0, ref1.getData().length)
    cachedLazyStruct2.init(ref2, 0, ref2.getData().length)
  }
  
  @Test
  def createENE1() {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull",id)
            )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct2)) 
  }

  @Test
  def createENE2() {
    var eval =
        CGEvaluatorFactory.get(
                new ExprNodeConstantDesc(111.asInstanceOf[Short])
            )

    eval.initialize(oi)
    assertEquals(new ShortWritable(111), eval.evaluate(cachedLazyStruct))
    assertEquals(new ShortWritable(111), eval.evaluate(cachedLazyStruct1))
    assertEquals(new ShortWritable(111), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE3() {
    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",new ExprNodeConstantDesc(40000.asInstanceOf[Short]),new ExprNodeConstantDesc(40000.asInstanceOf[Short]))
            )

    eval.initialize(oi)
    assertEquals(new ShortWritable(14464), eval.evaluate(cachedLazyStruct))
    assertEquals(new ShortWritable(14464), eval.evaluate(cachedLazyStruct1))
    assertEquals(new ShortWritable(14464), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE4() {
    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("-",new ExprNodeConstantDesc(4000.asInstanceOf[Short]),new ExprNodeConstantDesc(4000.asInstanceOf[Short]))
            )

    eval.initialize(oi)
    assertEquals(new ShortWritable(0), eval.evaluate(cachedLazyStruct))
    assertEquals(new ShortWritable(0), eval.evaluate(cachedLazyStruct1))
    assertEquals(new ShortWritable(0), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE5() {
     var eval = CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",new ExprNodeConstantDesc("abc_"),new ExprNodeConstantDesc("cde"))
            )

    eval.initialize(oi)
    assertEquals(new Text("abc_cde"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("abc_cde"), eval.evaluate(cachedLazyStruct1))
    assertEquals(new Text("abc_cde"), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE6() {
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",new ExprNodeConstantDesc("abc_"),name)
            )

    eval.initialize(oi)
    assertEquals(new Text("abc_james"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("abc_james"), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE7() {
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",name, new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(1)),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",name, new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(1))
            ))

    eval.initialize(oi)
    assertEquals(new Text("jj"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("jj"), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE8() {
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                    // TODO to make a void data
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                        new ExprNodeConstantDesc(TypeInfoFactory.voidTypeInfo, null)
                        //fake
                        , new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(1)),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",name, new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(1))
            ))

    eval.initialize(oi)
    assertEquals(null, eval.evaluate(cachedLazyStruct))
    assertEquals(null, eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE9() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    //  ((id>5 && id <15) && (substr(name, 0,4)>="james" && substr(name,0,4)<="qames")
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                        id,
                        new ExprNodeConstantDesc(5)
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
                        id,
                        new ExprNodeConstantDesc(15)
                    )
                 ),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">=",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",name, new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(4)),
                        new ExprNodeConstantDesc("iames")
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",name, new ExprNodeConstantDesc(0), new ExprNodeConstantDesc(4)),
                        new ExprNodeConstantDesc("qames")
                    )
                 )
            ))

    eval.initialize(oi)
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE10() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    //   ((age>25) || (id != 3)) && ((id !=3) && ((age>25 && age<=35)))
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                        age,
                        new ExprNodeConstantDesc(25)
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                        id,
                        new ExprNodeConstantDesc(3)
                    )
                 ),
                 TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                   TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                       id,
                       new ExprNodeConstantDesc(3)
                   ),
                   TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                          age,
                          new ExprNodeConstantDesc(25)
                      ),
                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=",
                          age,
                          new ExprNodeConstantDesc(35)
                      )
                   )
                 )
            ))

    eval.initialize(oi)
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE11() {
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%",
                age,
                new ExprNodeConstantDesc(10)
            )
        )

    eval.initialize(oi)
    assertEquals(new IntWritable(1), eval.evaluate(cachedLazyStruct))
    assertEquals(new IntWritable(1), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE12() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                        depart,
                        new ExprNodeConstantDesc(0),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%",
                            age,
                            new ExprNodeConstantDesc(10)
                        )
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                        name,
                        new ExprNodeConstantDesc(0),
                        new ExprNodeConstantDesc(1)
                    )
                )
        )

    eval.initialize(oi)
    assertEquals(new Text("sj"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("sj"), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE13() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
   // (   ((id<=-100 or id>=0) and (id>0 and age != 31))
   //       or
   //   ((age<=>id and not(age=id)) and (concat("abc_",substr(name,0,3%age)) > "abc_") and name>depart)
   // )
   // and (age is not null and salary>age*10)
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=", id, new ExprNodeConstantDesc(-100)),
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">=", id, new ExprNodeConstantDesc(0))
                        ),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", id, new ExprNodeConstantDesc(0)),
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", age, new ExprNodeConstantDesc(31))
                        )
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<=>", age, id),
                                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!",TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=",age, id))
                            ),

                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                                  new ExprNodeConstantDesc("abc_"),
                                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr", name, new ExprNodeConstantDesc(0),
                                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%", new ExprNodeConstantDesc(3), age)
                                  )
                              ),
                              new ExprNodeConstantDesc("abc_")
                            )
                        ),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", name, depart)
                    )
                ),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull",age),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                        salary,
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                            age,
                            new ExprNodeConstantDesc(10)
                        )
                    )
                )
            )
        )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  // test the stateful function
  @Test
  def createENE14() {
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence")
                )
        )

    eval.initialize(oi)
    assertEquals(new LongWritable(1), eval.evaluate(cachedLazyStruct))
    assertEquals(new LongWritable(4), eval.evaluate(cachedLazyStruct1))
    assertEquals(new LongWritable(9), eval.evaluate(cachedLazyStruct2))
  }

  // test the deterministic-less function
  @Test
  def createENE15() {
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("==",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand")
                ),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand")
                )
            )
        )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct1))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE16() {
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
                    new ExprNodeConstantDesc(1)
                ),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
                    new ExprNodeConstantDesc(1)
                )
            )
        )

    eval.initialize(oi)
    assertEquals(new LongWritable(2), eval.evaluate(cachedLazyStruct))
    assertEquals(new LongWritable(4), eval.evaluate(cachedLazyStruct1))
    assertEquals(new LongWritable(6), eval.evaluate(cachedLazyStruct2))
  }

  // test the duplicated null check.
  @Test
  def createENE17() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                    id,
                    new ExprNodeConstantDesc(1)
                ),
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<",
                    id,
                    new ExprNodeConstantDesc(4)
                )
            )
        )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct1))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct2))
  }

  // test the duplicated null check.
  @Test
  def createENE18() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "age", "a", false)

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                        id,
                        new ExprNodeConstantDesc(1)
                    ),
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                        id,
                        new ExprNodeConstantDesc(5)
                    )
                ),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                id,
                new ExprNodeConstantDesc(6)
            )
        )
    )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct2))
  }

  @Test
  def createENE19() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var fake = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "fake", "a", false)
    
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,

    var eval =
        CGEvaluatorFactory.get(
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
                    fake
                )
        )

    eval.initialize(oi)
    assertEquals(new Text("12_sales_james_31_1234.6_2012-03-28 17:25:58_"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("12_sales_james_31_1234.6_2012-03-28 17:25:58_"), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }
    
 // between
  @Test
  def createENE20() {
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("between",
                    new ExprNodeConstantDesc(false),
                    age,
                    new ExprNodeConstantDesc(30),
                    new ExprNodeConstantDesc(32)
                )
        )

    eval.initialize(oi)
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }
    
  //printf
  @Test
  def createENE21() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var fake = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "fake", "a", false)
    
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var format = "%d_%s_%s_%d_%.1f_%tF_%s"
    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("printf",
                    new ExprNodeConstantDesc(format),
                    id,
                    depart,
                    name,
                    age,
                    salary,
                    jointime,
                    fake
                )
        )
        
    eval.initialize(oi)
    assertEquals(new Text("12_sales_james_31_1234.6_2012-03-28_"), eval.evaluate(cachedLazyStruct))
    assertEquals(new Text("12_sales_james_31_1234.6_2012-03-28_"), eval.evaluate(cachedLazyStruct1))
    assertEquals(new Text("12_null_null_null_n_null_null"), eval.evaluate(cachedLazyStruct2))
  }
  
  // instr
  @Test
  def createENE22() {
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
                    name,
                    new ExprNodeConstantDesc("me")
                )
        )
        
    eval.initialize(oi)
    assertEquals(new IntWritable(3), eval.evaluate(cachedLazyStruct))
    assertEquals(new IntWritable(3), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }

  // instr
  @Test
  def createENE23() {
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("instr",
                    name,
                    new ExprNodeConstantDesc(3)
                )
        )
        
    eval.initialize(oi)
    assertEquals(new IntWritable(0), eval.evaluate(cachedLazyStruct))
    assertEquals(new IntWritable(0), eval.evaluate(cachedLazyStruct1))
    assertEquals(null, eval.evaluate(cachedLazyStruct2))
  }
  
  // combination
  @Test
  def createENE24() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var depart = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "depart", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    var age    = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "age", "a", false)
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
                        fake
                    ),
                  name
                )
    var eval = CGEvaluatorFactory.get(desc)
        
    eval.initialize(oi)
    
    assertEquals(new IntWritable(10), eval.evaluate(cachedLazyStruct))
    assertEquals(new IntWritable(10), eval.evaluate(cachedLazyStruct1))
    
    cachedLazyStruct2.init(ref2, 0, ref2.getData().length)
    var obj = eval.evaluate(cachedLazyStruct2)
    
    assertEquals(null, obj)
  }
  
  // combination
  @Test
  def createENE25() {
    FunctionRegistry.registerTemporaryUDF("row_sequence", classOf[UDFRowSequence], false)
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    // concat(row_sequence() > 3, concat(name, id)) 
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                 TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                   TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("row_sequence"),
                   new ExprNodeConstantDesc(13)
                 ),
                 TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                   name,
                   id
                 )
               )
    var eval = CGEvaluatorFactory.get(desc)
        
    eval.initialize(oi)
    
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(false), eval.evaluate(cachedLazyStruct1))
    
    cachedLazyStruct2.init(ref2, 0, ref2.getData().length)
    var obj = eval.evaluate(cachedLazyStruct2)
    
    assertEquals(null, obj)
  }
  
  // null in BinaryCodeGen
  @Test
  def createENE26() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    // name>id
    //12,sales,james,31,1234.6,2012-03-28 17:25:58,
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                 name,
                 id
               )
    var eval = CGEvaluatorFactory.get(desc)
        
    eval.initialize(oi)
    
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct))
    assertEquals(new BooleanWritable(true), eval.evaluate(cachedLazyStruct1))
    
    cachedLazyStruct2.init(ref2, 0, ref2.getData().length)
    var obj = eval.evaluate(cachedLazyStruct2)
    
    assertEquals(null, obj)
  }
  
  // cg failure
  @Test(expected=classOf[CGAssertRuntimeException])
  def createENE27() {
    var id     = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var name   = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    // map(name,id)
    //as Map is not supported yet, it will raise the exception.
    var desc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("map",
                 name,
                 id
               )
    var eval = CGEvaluatorFactory.get(desc)
        
    eval.initialize(oi)
  }
  
  // for main function test.
  def createEE2()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    // if (id > 0) and
    //   (name like 'abc' and name != 'abcd') and
    //   ((age is not null and age>1) or (salary>0))

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("like", name, new ExprNodeConstantDesc("name")),
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", name, new ExprNodeConstantDesc("abcd"))
                        ),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", id, new ExprNodeConstantDesc(0))),

                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull", age),
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", age, new ExprNodeConstantDesc(111))),

                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                            salary, new ExprNodeConstantDesc(0f)))
                    )
            )

     eval
  }

  def createEE()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)
    var age = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "age", "a", false)
    var salary = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "salary", "a", false)
    // if ((id < 3 ) and (id>0)) or ((age>1) and (salary>0))

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<", id,
                            new ExprNodeConstantDesc(3)),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", id,
                            new ExprNodeConstantDesc(0))),

                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", age,
                            new ExprNodeConstantDesc(1.asInstanceOf[Short])),
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">",
                            salary, new ExprNodeConstantDesc(0f)))
                    )
            )
     eval
  }

  def createEE3()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("isnotnull",id)
            )
     eval
  }

  def createEE4()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                new ExprNodeConstantDesc(1.asInstanceOf[Long]),
                new ExprNodeConstantDesc(1.asInstanceOf[Short])))
     eval
  }

  def createEE5()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                id,
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                    id,
                    new ExprNodeConstantDesc(1))))
     eval
  }

  def createEE6()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                new ExprNodeConstantDesc(0l),
                new ExprNodeConstantDesc(0l)
            )
        )
     eval
  }

  def createEE7()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*", new ExprNodeConstantDesc(0l), id),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("*", new ExprNodeConstantDesc(3l), id)
        ))
     eval
  }

  def createEE8()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(">", new ExprNodeConstantDesc(0l), id),
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("<", new ExprNodeConstantDesc(3l), id)
        ))
     eval
  }

  def createEE9()  = {
    var name = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)

    var eval =
        CGEvaluatorFactory.get(
                        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("like", name, new ExprNodeConstantDesc("name")),
                            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", name, new ExprNodeConstantDesc("abcd"))
                        )
            )

     eval
  }

  def createEE10()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "id", "a", false)

    var eval =
        CGEvaluatorFactory.get(
               TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("or",
                         TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", new ExprNodeConstantDesc(3), new ExprNodeConstantDesc(5)),
                         TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", id, new ExprNodeConstantDesc(3))),

                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=", id, new ExprNodeConstantDesc(7)))
            )

     eval
  }

  def createEE11()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "name", "a", false)
    // (substr(name,0,3) != "name1") && (substr(name,0,3) != "name2")
    var eval =
        CGEvaluatorFactory.get(
               TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("and",
                 TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                     TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                         id,
                         new ExprNodeConstantDesc(0),
                         new ExprNodeConstantDesc(3)),
                     new ExprNodeConstantDesc("name1")),

                 TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("!=",
                     TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("substr",
                         id,
                         new ExprNodeConstantDesc(0),
                         new ExprNodeConstantDesc(3)),
                     new ExprNodeConstantDesc("name2"))
              )
        )


     eval
  }

  def createEE12()  = {
    var id = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "id", "a", false)

    // id NOT between 3 and 4
    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("between",
                new ExprNodeConstantDesc(false),
                id,
                new ExprNodeConstantDesc(3),
                new ExprNodeConstantDesc(4)
            )
        )
     eval
  }

  def createEE13()  = {
    var jointime = new ExprNodeColumnDesc(TypeInfoFactory.timestampTypeInfo, "jointime", "a", false)
    var ts = new Timestamp(System.currentTimeMillis())

    var eval =
        CGEvaluatorFactory.get(
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("=",
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, ts),
                jointime
            )
        )
     eval
  }

  def testCG(times:Int, eval:ExprNodeEvaluator) = {
    try {
      //var eval = createEE5()
      var oi = createOI()
      eval.initialize(oi)

      var ref = createByteRef()

      var cachedLazyStruct = LazyFactory
          .createLazyObject(oi).asInstanceOf[LazyStruct]

      cachedLazyStruct.init(ref, 0, ref.getData().length)
      var s = System.currentTimeMillis()
      for (i <- 0 until times) {
        var obj = eval.evaluate(cachedLazyStruct)
      }
      var e = System.currentTimeMillis()
      printf ("Running:%d ms for %d times\n",(e - s), times)
    } catch {
      case e:Throwable => e.printStackTrace()
      throw e
    }
  }

//  def testCG2(times:Int) = {
//    try {
//      var eval = new AB()
//      eval.init(oi)
//      cachedLazyStruct.init(ref, 0, ref.getData().length)
//      var s = System.currentTimeMillis()
//      for (i <- 0 to times) {
//        var obj = eval.evaluate(cachedLazyStruct)
//        println(obj)
//      }
//      var e = System.currentTimeMillis()
//      println ("CG:%d for %d\n".format((e - s), times))
//    } catch {
//      case e => e.printStackTrace()
//      throw e
//    }
//  }
}

object Run extends App {
  override def main(args:Array[String]) {
    var expr = new TestExprNodeCodeGen()
    
    val times = 1
//    expr.testCG2(times)
    expr.testCG(times, expr.createEE12())
    expr.testCG(times, expr.createEE13())
    expr.testCG(times, expr.createEE11())
    expr.testCG(times, expr.createEE10())
    expr.testCG(times, expr.createEE8())
    expr.testCG(times, expr.createEE7())
    expr.testCG(times, expr.createEE())
    expr.testCG(times, expr.createEE2())
    expr.testCG(times, expr.createEE3())
    expr.testCG(times, expr.createEE4())
    expr.testCG(times, expr.createEE5())
    expr.testCG(times, expr.createEE6())
    expr.testCG(times, expr.createEE9())
    
  }
}

