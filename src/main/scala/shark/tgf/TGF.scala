/*
 * Copyright (C) 2013 The Regents of The University California.
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

package shark.tgf

import java.sql.Timestamp
import java.util.Date

import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}
import scala.util.parsing.combinator._

import org.apache.spark.rdd.RDD

import shark.api._
import shark.SharkContext
import java.lang.reflect.Method

/**
 * This object is responsible for handling TGF (Table Generating Function) commands.
 *
 * {{{
 * -- TGF Commands --
 * GENERATE tgfname(param1, param2, ... , param_n)
 * GENERATE tgfname(param1, param2, ... , param_n) AS tablename
 * }}}
 *
 * Parameters can either be of primitive types, eg int, or of type RDD[Product]. TGF.execute()
 * will use reflection looking for an object of name "tgfname", invoking apply() with the
 * primitive values. If the type of a parameter to apply() is RDD[Product], it will assume the
 * parameter is the name of a table, which it will turn into an RDD before invoking apply().
 *
 * For example, "GENERATE MyObj(25, emp)" will invoke
 * MyObj.apply(25, sc.sql2rdd("select * from emp"))
 * , assuming the TGF object (MyObj) has an apply function that takes an int and an RDD[Product].
 *
 * The "as" version of the command saves the output in a new table named "tablename",
 * whereas the other version returns a ResultSet.
 *
 * -- Defining TGF objects --
 * TGF objects need to have an apply() function and take an arbitrary number of either primitive
 * or RDD[Product] typed parameters. The apply() function should either return an RDD[Product]
 * or RDDSchema. When the former case is used, the returned table's schema and column names need
 * to be defined through a Java annotation called @Schema. Here is a short example:
 * {{{
 * object MyTGF1 {
 *  \@Schema(spec = "name string, age int")
 *   def apply(table1: RDD[(String, String, Int)]): RDD[Product] = {
 *     // code that manipulates table1 and returns a new RDD of tuples
 *   }
 * }
 * }}}
 *
 * Sometimes, the TGF dynamically determines the number or types of columns returned. In this case,
 * the TGF can use the RDDSchema return type instead of Java annotations. RDDSchema simply contains
 * a schema string and an RDD of results. For example:
 * {{{
 * object MyTGF2 {
 *   \@Schema(spec = "name string, age int")
 *   def apply(table1: RDD[(String, String, Int)]): RDD[Product] = {
 *     // code that manipulates table1 and creates a result rdd
 *     return RDDSchema(rdd.asInstanceOf[RDD[Seq[_]]], "name string, age int")
 *   }
 * }
 * }}}
 *
 * Sometimes the TGF needs to internally make SQL calls. For that, it needs access to a
 * SharkContext object. Therefore,
 * {{{
 * def apply(sc: SharkContext, table1: RDD[(String, String, Int)]): RDD[Product] = {
 *   // code that can use sc, for example by calling sc.sql2rdd()
 *   // code that manipulates table1 and returns a new RDD of tuples
 * }
 * }}}
 */

object TGF {
  private val parser = new TGFParser

  /**
   * Executes a TGF command and gives back the ResultSet.
   * Mainly to be used from SharkContext (e.g. runSql())
   *
   * @param sql TGF command, e.g. "GENERATE name(params) AS tablename"
   * @param sc SharkContext
   * @return ResultSet containing the results of the command
   */
  def execute(sql: String, sc: SharkContext): ResultSet = {
    val ast = parser.parseAll(parser.tgf, sql).getOrElse(
      throw new QueryExecutionException("TGF parse error: "+ sql))

    val (tableNameOpt, tgfName, params) = ast match {
      case (tgfName, params) =>
        (None, tgfName.asInstanceOf[String], params.asInstanceOf[List[String]])
      case (tableName, tgfName, params) =>
        (Some(tableName.asInstanceOf[String]), tgfName.asInstanceOf[String],
          params.asInstanceOf[List[String]])
    }

    val obj = reflectInvoke(tgfName, params, sc)
    val (rdd, schema) = getSchema(obj, tgfName)

    val (sharkSchema, resultArr) = tableNameOpt match {
      case Some(tableName) =>  // materialize results
        val helper = new RDDTableFunctions(rdd, schema.map { case (_, tpe) => toClassTag(tpe) })
        helper.saveAsTable(tableName, schema.map{ case (name, _) => name})
        (Array[ColumnDesc](), Array[Array[Object]]())
      case None =>  // return results
        val newSchema = schema.map { case (name, tpe) =>
          new ColumnDesc(name, DataTypes.fromClassTag(toClassTag(tpe)))
        }
        val res = rdd.collect().map{p => p.map( _.asInstanceOf[Object] ).toArray}
        (newSchema.toArray, res)
    }
    new ResultSet(sharkSchema, resultArr)
  }

  private def getMethod(tgfName: String, methodName: String): Option[Method] = {
    val tgfClazz = try {
      Thread.currentThread().getContextClassLoader.loadClass(tgfName)
    } catch {
      case ex: ClassNotFoundException =>
        throw new QueryExecutionException("Couldn't find TGF class: " + tgfName)
    }

    val methods = tgfClazz.getDeclaredMethods.filter(_.getName == methodName)
    if (methods.isEmpty) None else Some(methods(0))
  }

  private def getSchema(tgfOutput: Object, tgfName: String): (RDD[Seq[_]], Seq[(String,String)]) = {
    tgfOutput match {
      case rddSchema: RDDSchema =>
        val schema = parser.parseAll(parser.schema, rddSchema.schema)

        (rddSchema.rdd, schema.get)
      case rdd: RDD[Product] =>
        val applyMethod = getMethod(tgfName, "apply")
        if (applyMethod == None) {
          throw new QueryExecutionException("TGF lacking apply() method")
        }

        val annotations = applyMethod.get.getAnnotation(classOf[Schema])
        if (annotations == null || annotations.spec() == null) {
          throw new QueryExecutionException("No schema annotation found for TGF")
        }

        // TODO: How can we compare schema with None?
        val schema = parser.parseAll(parser.schema, annotations.spec())
        if (schema.isEmpty) {
          throw new QueryExecutionException(
            "Error parsing TGF schema annotation (@Schema(spec=...)")
        }

        (rdd.map(_.productIterator.toList), schema.get)
      case _ =>
        throw new QueryExecutionException("TGF output needs to be of type RDD or RDDSchema")
    }
  }

  private def reflectInvoke(tgfName: String, paramStrs: Seq[String], sc: SharkContext) = {

    val applyMethodOpt = getMethod(tgfName, "apply")
    if (applyMethodOpt.isEmpty) {
      throw new QueryExecutionException("TGF " + tgfName + " needs to implement apply()")
    }

    val applyMethod = applyMethodOpt.get

    val typeNames: Seq[String] = applyMethod.getParameterTypes.toList.map(_.toString)

    val augParams =
      if (!typeNames.isEmpty && typeNames.head.startsWith("class shark.SharkContext")) {
        Seq("sc") ++ paramStrs
      } else {
        paramStrs
      }

    if (augParams.length != typeNames.length) {
      throw new QueryExecutionException("Expecting " + typeNames.length +
        " parameters to " + tgfName + ", got " + augParams.length)
    }

    val params = (augParams.toList zip typeNames.toList).map {
      case (param: String, tpe: String) if tpe.startsWith("class shark.SharkContext") =>
        sc
      case (param: String, tpe: String) if tpe.startsWith("class org.apache.spark.rdd.RDD") =>
        tableRdd(sc, param)
      case (param: String, tpe: String) if tpe.startsWith("long") =>
        param.toLong
      case (param: String, tpe: String) if tpe.startsWith("int") =>
        param.toInt
      case (param: String, tpe: String) if tpe.startsWith("double") =>
        param.toDouble
      case (param: String, tpe: String) if tpe.startsWith("float") =>
        param.toFloat
      case (param: String, tpe: String) if tpe.startsWith("class java.lang.String") ||
          tpe.startsWith("class String") =>
        param.stripPrefix("\"").stripSuffix("\"")
      case (param: String, tpe: String) =>
        throw new QueryExecutionException(s"Expected TGF parameter type: $tpe ($param)")
    }

    applyMethod.invoke(null, params.asInstanceOf[List[Object]] : _*)
  }

  private def toClassTag(tpe: String): ClassTag[_] = {
    if (tpe == "boolean") classTag[Boolean]
    else if (tpe == "tinyint") classTag[Byte]
    else if (tpe == "smallint") classTag[Short]
    else if (tpe == "int") classTag[Integer]
    else if (tpe == "bigint") classTag[Long]
    else if (tpe == "float") classTag[Float]
    else if (tpe == "double") classTag[Double]
    else if (tpe == "string") classTag[String]
    else if (tpe == "timestamp") classTag[Timestamp]
    else if (tpe == "date") classTag[Date]
    else {
      throw new QueryExecutionException("Unknown column type specified in schema (" + tpe + ")")
    }
  }

  def tableRdd(sc: SharkContext, tableName: String): RDD[_] = {
    val rdd = sc.sql2rdd("SELECT * FROM " + tableName)
    rdd.schema.size match {
      case 2 => new TableRDD2(rdd, Seq())
      case 3 => new TableRDD3(rdd, Seq())
      case 4 => new TableRDD4(rdd, Seq())
      case 5 => new TableRDD5(rdd, Seq())
      case 6 => new TableRDD6(rdd, Seq())
      case 7 => new TableRDD7(rdd, Seq())
      case 8 => new TableRDD8(rdd, Seq())
      case 9 => new TableRDD9(rdd, Seq())
      case 10 => new TableRDD10(rdd, Seq())
      case 11 => new TableRDD11(rdd, Seq())
      case 12 => new TableRDD12(rdd, Seq())
      case 13 => new TableRDD13(rdd, Seq())
      case 14 => new TableRDD14(rdd, Seq())
      case 15 => new TableRDD15(rdd, Seq())
      case 16 => new TableRDD16(rdd, Seq())
      case 17 => new TableRDD17(rdd, Seq())
      case 18 => new TableRDD18(rdd, Seq())
      case 19 => new TableRDD19(rdd, Seq())
      case 20 => new TableRDD20(rdd, Seq())
      case 21 => new TableRDD21(rdd, Seq())
      case 22 => new TableRDD22(rdd, Seq())
      case _ => new TableSeqRDD(rdd)
    }
  }
}

case class RDDSchema(rdd: RDD[Seq[_]], schema: String)

private class TGFParser extends JavaTokenParsers {

  // Code to enable case-insensitive modifiers to strings, e.g.
  // "Berkeley".ci will match "berkeley"
  class MyString(str: String) {
    def ci: Parser[String] = ("(?i)" + str).r
  }

  implicit def stringToRichString(str: String): MyString = new MyString(str)

  def tgf: Parser[Any] = saveTgf | basicTgf

  /**
   * @return Tuple2 containing a TGF method name and a List of parameters as strings
   */
  def basicTgf: Parser[(String, List[String])] = {
    ("GENERATE".ci  ~> methodName) ~ (("(" ~> repsep(param, ",")) <~ ")") ^^
      { case id1 ~ x => (id1, x.asInstanceOf[List[String]]) }
  }

  /**
   * @return Tuple3 containing a table name, TGF method name and a List of parameters as strings
   */
  def saveTgf: Parser[(String, String, List[String])] = {
    (("GENERATE".ci ~> methodName) ~ (("(" ~> repsep(param, ",")) <~ ")")) ~ (("AS".ci) ~>
      ident) ^^ { case id1 ~ x ~ id2 => (id2, id1, x.asInstanceOf[List[String]]) }
  }

  def schema: Parser[Seq[(String,String)]] = repsep(nameType, ",")

  def nameType: Parser[(String,String)] = ident ~ ident ^^ { case name~tpe => Tuple2(name, tpe) }

  def param: Parser[Any] = stringLiteral | floatingPointNumber | decimalNumber | ident |
    failure("Expected a string, number, or identifier as parameters in TGF")

  def methodName: Parser[String] = """[a-zA-Z_][\w\.]*""".r
}
