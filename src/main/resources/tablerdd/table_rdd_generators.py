#!/usr/bin/python
from string import Template
import sys

## This script generates the 22 functions needed to create sqlToRdd's

p = sys.stdout

# e.g. createList(1,3, "T[", "]", ",") gives T[1],T[2],T[3]
def createList(start, stop, prefix, suffix="", sep = ",", newlineAfter = 80, indent = 0):
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
for x in range(2,23):

    inner = ""
    for y in range(1,x+1):
        if y % 3 == 1: inner += "       "
        inner += Template(" row.getPrimitiveGeneric[T$num1]($num2)").substitute(num1=y, num2=y-1)
        if y != x: inner += ","
        if y % 3 == 0: inner += "\n"
    inner += " ) )\n"

    tableClass = Template(
"""
class TableRDD$num[$list](prev: RowRDD,
                       mans: Seq[ClassManifest[_]])
  extends RDD[Tuple$num[$list]](prev) {
  def schema = prev.schema

  val tableCols = schema.size
  if (tableCols != $num) throw new IllegalArgumentException("Table only has " + tableCols + " columns, expecting $num")

  mans.zipWithIndex.foreach{ case (m, i) => if (DataTypes.fromManifest(m) != schema(i).dataType)
    throw new IllegalArgumentException(
      "Type mismatch on column " + (i + 1) + ", expected " + DataTypes.fromManifest(m) + " got " + schema(i).dataType) }

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
  
# The SharkContext declarations
for x in range(2,23):
    sqlRddFun = Template(
"""
  def sqlRdd[$list1](cmd: String):
    RDD[Tuple$num[$list2]] =
    new TableRDD$num[$list2](sqlRowRdd(cmd),
      Seq($list3))
""").substitute(num = x, 
               list1 = createList(1, x, "T", ": M", ", ", 80, 4), 
               list2 = createList(1, x, "T", sep=", ", indent = 4), 
               list3 = createList(1, x, "m[T", "]", sep=", ", indent = 10))
    p.write(sqlRddFun)
