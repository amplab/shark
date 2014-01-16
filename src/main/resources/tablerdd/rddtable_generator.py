#!/usr/bin/python
from string import Template
import sys
from generator_utils import *

## This script generates RDDtable.scala

p = sys.stdout

# e.g. createList(1,3, "T[", "]", ",") gives T[1],T[2],T[3]
def createList(start, stop, prefix, suffix="", sep = ",", newlineAfter = 70, indent = 0):
    res = ""
    oneLine = res
    for y in range(start,stop+1):
        res     += prefix + str(y) + suffix
        oneLine += prefix + str(y) + suffix
        if y != stop:
            res     += sep
            oneLine += sep
            if len(oneLine) > newlineAfter:
                res += "\n" + " "*indent
                oneLine = ""
    return res

### The SparkContext declaration

prefix = """
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

package shark.api

// *** This file is auto-generated from RDDTable_generator.py ***

import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object RDDTableImplicits {
  private type C[T] = ClassTag[T]

"""

p.write(prefix)

for x in range(2,23):

    tableClass = Template(
"""
  implicit def rddToTable$num[$tmlist]
  (rdd: RDD[($tlist)]): RDDTableFunctions = RDDTable(rdd)

""").substitute(num = x, tmlist = createList(1, x, "T", ": C", ", ", indent=4), tlist = createList(1, x, "T", "", ", ", indent=4))
    p.write(tableClass)

prefix = """
}

object RDDTable {

  private type C[T] = ClassTag[T]
  private def ct[T](implicit c: ClassTag[T]) = c
"""

p.write(prefix)

for x in range(2,23):

    tableClass = Template(
"""
  def apply[$tmlist]
  (rdd: RDD[($tlist)]) = {
    val classTag = implicitly[ClassTag[Seq[Any]]]
    val rddSeq: RDD[Seq[_]] = rdd.map(t => t.productIterator.toList.asInstanceOf[Seq[Any]])(classTag)
    new RDDTableFunctions(rddSeq, Seq($mtlist))
  }

""").substitute(tmlist = createList(1, x, "T", ": C", ", ", indent=4), tlist = createList(1, x, "T", "", ", ", indent=4),
                mtlist = createList(1, x, "ct[T", "]", ", ", indent=4))
    p.write(tableClass)


p.write("}\n")
