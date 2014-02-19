package shark
package execution
package cg

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

package object row {
  type DataType = CGField[_<:ObjectInspector]
  
//  import scala.language.implicitConversions
      
  /**
   * Generate source code for initializing the constant Text value, as byte array
   */
  implicit def textConvert2ByteArrayInHex(writable: Text): String =
    if (writable != null)
      "new String(new byte[]{" +
        (for (i <- 0 to writable.getLength() - 1)
          yield "(byte)0x" + Integer.toHexString(0xFF & writable.getBytes()(i))).
        reduce(_ + "," + _) +
        "})"
    else
      "null"
      
  implicit def textConvert2ByteArrayInHex(w: String): String =
    if (w != null)
      "new String(new byte[]{" +
        w.map(b => "(byte)0x" + Integer.toHexString(0xFF & b)).reduce(_ + "," + _) +
        "})"
    else
      "null"      

  /**
   * Generate source code for initializing the constant byte array
   */
  implicit def bytesConvert2HexString(bytes: Array[Byte]): String =
    if (bytes != null) {
      "new byte[]{" +
        (for (i <- 0 to bytes.length - 1)
          yield "(byte)0x" + Integer.toHexString(0xFF & bytes(i))).reduce(_ + "," + _) +
        "}"
    } else
      "null"
      
  implicit def bytesConvert2ByteArrayInHex(writable: BytesWritable): String = 
    if (writable != null)
      "new byte[]{" +
        (for (i <- 0 to writable.getLength() - 1)
          yield "(byte)0x" + Integer.toHexString(0xFF & writable.getBytes()(i))).
        reduce(_ + "," + _) +
        "}"
    else
      "null"

  implicit def timestampConvert2ByteArrayInHex(w: java.sql.Timestamp): String =
    if (w != null)
      "new java.sql.Timestamp(" + w.getTime() + "l)"
    else
      "null"
  implicit def timestampConvert2ByteArrayInHex(writable: TimestampWritable): String =
    if (writable != null)
      timestampConvert2ByteArrayInHex(writable.getTimestamp())
    else
      "null"      

  implicit def booleanConvert2(w: java.lang.Boolean): String = 
    if(w != null) 
      w.toString()
    else
      "false" 
  implicit def booleanConvert2(writable: BooleanWritable): String = 
    if(writable != null) 
      booleanConvert2(writable.get())
    else
      "false"
  
  implicit def byteConvert2(w: java.lang.Byte): String = 
    if(w != null) 
      "(byte)%s".format(w)
    else
      "(byte)0"
  implicit def byteConvert2(writable: ByteWritable): String = 
    if(writable != null) 
      byteConvert2(writable.get())
    else
      "(byte)0"      
  
  implicit def doubleConvert2(w: java.lang.Double): String = 
    if(w != null) 
      "%sd".format(w)
    else
      "0.0d"
  implicit def doubleConvert2(writable: DoubleWritable): String = 
    if(writable != null) 
      doubleConvert2(writable.get())
    else
      "0.0d"      
  
  implicit def floatConvert2(w: java.lang.Float): String = 
    if(w != null) 
      "%sf".format(w)
    else
      "0.0f"
  implicit def floatConvert2(writable: FloatWritable): String = 
    if(writable != null) 
      floatConvert2(writable.get())
    else
      "0.0f"      
  
  implicit def intConvert2(w: java.lang.Integer): String = 
    if(w != null) 
      "%s".format(w)
    else
      "0"
      
  implicit def intConvert2(writable: IntWritable): String = 
    if(writable != null) 
      intConvert2(writable.get())
    else
      "0"      
  
  implicit def longConvert2(w: java.lang.Long): String = 
    if(w != null) 
      "%sl".format(w)
    else
      "0l"
  implicit def longConvert2(writable: LongWritable): String = 
    if(writable != null) 
      longConvert2(writable.get())
    else
      "0l"      

  implicit def shortConvert2(w: java.lang.Short): String = 
    if(w != null) 
      "(short)%s".format(w)
    else
      "(short)0"
      
  implicit def shortConvert2(writable: ShortWritable): String = 
    if(writable != null) 
      shortConvert2(writable.get())
    else
      "(short)0"
}