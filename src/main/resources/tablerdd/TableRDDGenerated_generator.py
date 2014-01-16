#!/usr/bin/python
from string import Template
import sys
from generator_utils import *

## This script generates TableRDDGenerated.scala

p = sys.stdout

p.write(
"""
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



package shark.api

// *** This file is auto-generated from TableRDDGenerated_generator.py ***
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition}

import scala.reflect.ClassTag

class TableSeqRDD(prev: TableRDD)
  extends RDD[Seq[Any]](prev) {

  def getSchema = prev.schema

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[Any]] = {
    prev.compute(split, context).map( row =>
      (0 until prev.schema.size).map(i => row.getPrimitive(i)) )
  }
}

""")

for x in range(1,23):

    inner = ""
    for y in range(1,x+1):
        if y % 3 == 1: inner += "       "
        inner += Template(" row.getPrimitiveGeneric[T$num1]($num2)").substitute(num1=y, num2=y-1)
        if y != x: inner += ","
        if y % 3 == 0: inner += "\n"
    inner += " ) )\n"

    tableClass = Template(
"""
class TableRDD$num[$list](prev: TableRDD,
                          tags: Seq[ClassTag[_]])
  extends RDD[Tuple$num[$list]](prev) {
  def schema = prev.schema

  private val tableCols = schema.size
  require(tableCols == $num, "Table only has " + tableCols + " columns, expecting $num")

  tags.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromClassTag(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromClassTag(m) + " got " + schema(i).dataType) }

  override def getPartitions = prev.getPartitions

  override def compute(split: Partition, context: TaskContext):
  Iterator[Tuple$num[$list]] = {
    prev.compute(split, context).map( row =>
      new Tuple$num[$list](
        $innerfatlist
  }
}
""").substitute(num = x, list = createList(1, x, "T", "", ", ", indent=4), innerfatlist = inner)


    p.write(tableClass)
