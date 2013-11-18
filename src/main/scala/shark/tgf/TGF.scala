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
import java.util.{Random, Date}

import scala.util.parsing.combinator._

import org.apache.spark.rdd.RDD

import shark.api._
import shark.SharkContext
import scala.Tuple3
import scala.Some
import scala.Tuple2


private class TGFParser extends JavaTokenParsers {

  /* Code to enable case-insensitive modifiers to strings, e.g. "DataBricks".ci will match "databricks" */
  class MyString(str: String) {
    def ci: Parser[String] = ("(?i)" + str).r
  }

  implicit def stringToRichString(str: String): MyString = new MyString(str)

  def tgf: Parser[Any] = saveTgf | basicTgf

  /**
   * @return Tuple2 containing a TGF method name and a List of parameters as strings
   */
  def basicTgf: Parser[Tuple2[String, List[String]]] = {
    ("GENERATE".ci  ~> methodName) ~ (("(" ~> repsep(param, ",")) <~ ")") ^^
      { case id1 ~ x => (id1, x.asInstanceOf[List[String]]) }
  }

  /**
   * @return Tuple3 containing a table name, TGF method name and a List of parameters as strings
   */
  def saveTgf: Parser[Tuple3[String, String, List[String]]] = {
    (("GENERATE".ci ~> methodName) ~ (("(" ~> repsep(param, ",")) <~ ")")) ~ (("SAVE".ci ~ "AS".ci) ~>
      ident) ^^ { case id1 ~ x ~ id2 => (id2, id1, x.asInstanceOf[List[String]]) }
  }
  
  def schema: Parser[List[Tuple2[String,String]]] = repsep(nameType, ",")

  def nameType: Parser[Tuple2[String,String]] = ident ~ ident ^^ { case name~tpe => Tuple2(name, tpe) }

  def param: Parser[Any] = stringLiteral | floatingPointNumber | decimalNumber | ident |
    failure("Expected a string, number, or identifier as parameters in TGF")

  def methodName: Parser[String] =
    """[a-zA-Z_][\w\.]*""".r
}

object TGF {

  private val parser = new TGFParser

  private def reflectInvoke(tgfName: String, paramStrs: Seq[String], sc: SharkContext):
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

  def execute(sql: String, sc: SharkContext): ResultSet = {
    val ast = parser.parseAll(parser.tgf, sql).getOrElse{throw new QueryExecutionException("TGF parse error: "+ sql)}

    val (tableNameOpt, tgfName, params) = ast match {
      case Tuple2(tgfName, params) => (None, tgfName.asInstanceOf[String], params.asInstanceOf[List[String]])
      case Tuple3(tableName, tgfName, params) => (Some(tableName.asInstanceOf[String]), tgfName.asInstanceOf[String],
        params.asInstanceOf[List[String]])
    }

    val (resultRdd, schema) = reflectInvoke(tgfName, params, sc)

    val (sharkSchema, resultArr) = tableNameOpt match {

      case Some(tableName) =>  // materialize results
      val helper = new RDDTableFunctions(resultRdd, schema.map{ case (_, tpe) => toManifest(tpe)})
      helper.saveAsTable(tableName, schema.map{ case (name, _) => name})
      (Array[ColumnDesc](), Array[Array[Object]]())

      case None =>  // return results
      val newSchema = schema.map{ case (name, tpe) => new ColumnDesc(name, DataTypes.fromManifest(toManifest(tpe)))}
      val res = resultRdd.collect().map{p => p.productIterator.map( _.asInstanceOf[Object] ).toArray}
      (newSchema.toArray, res)
    }
    new ResultSet(sharkSchema, resultArr)
  }

  private def toManifest(tpe: String): ClassManifest[_] = {
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


// Example TGF, should be removed.
object Clustering {
  import org.apache.spark.mllib.clustering._

  @Schema(spec = "year int, state string, product string, sales double, cluster int")
  def apply(sales: RDD[(Int, String, String, Double)], k: Int):
  RDD[(Int, String, String, Double, Int)] = {

    val dataset = sales.map{ case (year, state, product, sales) => Array[Double](sales) }
    val model = KMeans.train(dataset, k, 2, 2)

    sales.map{ case (year, state, product, sales) => (year, state, product, sales, model.predict(Array(sales))) }
  }
}