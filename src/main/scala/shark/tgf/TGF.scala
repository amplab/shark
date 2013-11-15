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

import scala.util.parsing.combinator._

import org.apache.spark.rdd.RDD

import shark.api.{QueryExecutionException, RDDTableFunctions}
import shark.SharkContext


class TGFParser extends JavaTokenParsers {

  /* Code to enable case-insensitive modifiers to strings, e.g. "DataBricks".ci will match "databricks" */
  class MyString(str: String) {
    def ci: Parser[String] = ("(?i)" + str).r
  }

  implicit def stringToRichString(str: String): MyString = new MyString(str)

  def gentgf: Parser[Tuple3[String, String, List[String]]] = {
    (((("GENERATE".ci ~ "TABLE".ci) ~> ident) <~ "USING".ci) ~ methodName) ~ (("(" ~> repsep(param, ",")) <~ ")") ^^
      { case (id1 ~ id2) ~ x => (id1, id2, x.asInstanceOf[List[String]]) }
  }

  def schema: Parser[List[Tuple2[String,String]]] = repsep(nameType, ",")

  def nameType: Parser[Tuple2[String,String]] = ident ~ ident ^^ { case name~tpe => Tuple2(name, tpe) }

  def param: Parser[Any] = stringLiteral | floatingPointNumber | decimalNumber | ident |
    failure("Expected a string, number, or identifier as parameters in TGF")

  def methodName: Parser[String] =
    """[a-zA-Z_][\w\.]*""".r
}

// Example TGF, should be removed.
object KulTGF {
  @Schema(spec = "name string, year int")
  def apply(t1: RDD[(Int, String)], n: Int): RDD[(String, Int)] = {
    t1.map{ case (i, s) => Tuple2(s, (i + n))}
  }
}

object TGF {

  val parser = new TGFParser

  def runTGF(tgfName: String, paramStrs: Seq[String], sc: SharkContext):
  Tuple2[RDD[Product], Seq[Tuple2[String,String]]] = {
    val tgfClazz = try {
      this.getClass.getClassLoader.loadClass(tgfName)
    } catch {
      case ex: ClassNotFoundException => throw new QueryExecutionException("Couldn't find TGF class: " + tgfName)
    }

    val methods = tgfClazz.getDeclaredMethods.filter(_.getName == "apply")

    if (methods.length < 1) throw new QueryExecutionException("TGF " + tgfName + " needs to implement apply()")

    val method: java.lang.reflect.Method = methods(0)

    val typeNames: Seq[String] = method.getParameterTypes.toList.map(_.toString)

    val annotations = method.getAnnotation(classOf[Schema]).spec()
    val colSchema = parser.parseAll(parser.schema, annotations).getOrElse(
      throw new QueryExecutionException("Error parsing TGF schema annotation (@Schema(spec=...)"))

    if (colSchema.length != typeNames.length)
      throw new QueryExecutionException("Need schema annotation with " + typeNames.length + " columns")


    if (paramStrs.length != typeNames.length) throw new QueryExecutionException("Expecting " + typeNames.length +
      " parameters to " + tgfName + ", got " + paramStrs.length)

    val params = (paramStrs.toList zip typeNames.toList).map {
      case (param: String, tpe: String) if (tpe.startsWith("class org.apache.spark.rdd.RDD")) => sc.tableRdd(param)
      case (param: String, tpe: String) if (tpe.startsWith("long")) => param.toLong
      case (param: String, tpe: String) if (tpe.startsWith("int")) => param.toInt
      case (param: String, tpe: String) if (tpe.startsWith("double")) => param.toDouble
      case (param: String, tpe: String) if (tpe.startsWith("float")) => param.toFloat
      case (param: String, tpe: String) if (tpe.startsWith("class String")) => param
      case (param: String, tpe: String) => throw
        new QueryExecutionException("Expected TGF parameter type: " + tpe + " (" + param + ")")
    }

    val tgfRes: RDD[Product] = method.invoke(null, params.asInstanceOf[List[Object]]:_*).asInstanceOf[RDD[Product]]

    Tuple2(tgfRes, colSchema)
  }

  def parseInvokeTGF(sql: String, sc: SharkContext): Tuple3[RDD[_], String, Seq[Tuple2[String,String]]] = {
    val ast = parser.parseAll(parser.gentgf, sql).getOrElse{throw new QueryExecutionException("TGF parse error: "+ sql)}
    val tableName = ast._1
    val tgfName = ast._2
    val paramStrings = ast._3
    val (rdd, schema) = runTGF(tgfName, paramStrings, sc)

    val helper = new RDDTableFunctions(rdd, schema.map{ case (_, tpe) => toManifest(tpe)})
    helper.saveAsTable(tableName, schema.map{ case (name, _) => name})
    (rdd, tableName, schema)
  }

  def toManifest(tpe: String): ClassManifest[_] = {
    if (tpe == "boolean") classManifest[java.lang.Boolean]
    else if (tpe == "tinyint") classManifest[java.lang.Byte]
    else if (tpe == "smallint") classManifest[java.lang.Short]
    else if (tpe == "int") classManifest[java.lang.Integer]
    else if (tpe == "bigint") classManifest[java.lang.Long]
    else if (tpe == "float") classManifest[java.lang.Float]
    else if (tpe == "double") classManifest[java.lang.Double]
    else if (tpe == "string") classManifest[java.lang.String]
    else if (tpe == "timestamp") classManifest[Timestamp]
    else if (tpe == "date") classManifest[Date]
    else throw new QueryExecutionException("Unknown column type specified in schema (" + tpe + ")")
  }
}
