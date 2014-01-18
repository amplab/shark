#!/usr/bin/python
from string import Template
import sys

from generator_utils import *

## This script generates functions sqlRdd for SharkContext.scala

p = sys.stdout

# The SharkContext declarations
for x in range(2,23):
    sqlRddFun = Template(
"""
  def sqlRdd[$list1](cmd: String):
    RDD[Tuple$num[$list2]] = {
    new TableRDD$num[$list2](sql2rdd(cmd),
      Seq($list3))
  }
""").substitute(num = x, 
               list1 = createList(1, x, "T", ": M", ", ", 80, 4), 
               list2 = createList(1, x, "T", sep=", ", indent = 4), 
               list3 = createList(1, x, "m[T", "]", sep=", ", indent = 10))
    p.write(sqlRddFun)
