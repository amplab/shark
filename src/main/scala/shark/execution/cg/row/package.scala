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

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable

package object row {
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

  /**
   * Generate source code for initializing the constant BytesWritable value, as byte array
   */
  implicit def bytesConvert2ByteArrayInHex(writable: BytesWritable) = 
    if (writable != null)
      "new byte[]{" +
        (for (i <- 0 to writable.getLength() - 1)
          yield "(byte)0x" + Integer.toHexString(0xFF & writable.getBytes()(i))).
        reduce(_ + "," + _) +
        "}"
    else
      "null"

  /**
   * Generate source code for initializing the constant TimestampWritable value, as 
   * Timestamp
   */        
  implicit def timestampConvert2ByteArrayInHex(writable: TimestampWritable) =
    if (writable != null)
      "new java.sql.Timestamp(" + writable.getTimestamp().getTime() + "l)"
    else
      "null"

  /**
   * Generate source code for initializing the constant DateWritable value, as Date
   */
  implicit def dateConvert2ByteArrayInHex(writable: DateWritable) =
    if (writable != null)
      "new java.sql.Date(" + writable.get().getTime() + "l)"
    else
      "null"
      
  implicit def booleanConvert2(writable: BooleanWritable) = 
    if(writable != null) 
      writable.get().toString()
    else
      "false"
  
  implicit def byteConvert2(writable: ByteWritable) = 
    if(writable != null) 
      "(byte)%s".format(writable.get())
    else
      "(byte)0"
  
  implicit def doubleConvert2(writable: DoubleWritable) = 
    if(writable != null) 
      "%sd".format(writable.get())
    else
      "0.0d"
  
  implicit def floatConvert2(writable: FloatWritable) = 
    if(writable != null) 
      "%sf".format(writable.get())
    else
      "0.0f"
  
  implicit def intConvert2(writable: IntWritable) = 
    if(writable != null) 
      "%s".format(writable.get())
    else
      "0"
  
  implicit def longConvert2(writable: LongWritable) = 
    if(writable != null) 
      "%sl".format(writable.get())
    else
      "0l"
  
  implicit def shortConvert2(writable: ShortWritable) = 
    if(writable != null) 
      "(short)%s".format(writable.get())
    else
      "(short)0"
}