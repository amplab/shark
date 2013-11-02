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

package shark.execution.serialization

import java.beans.{XMLDecoder, XMLEncoder}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.ning.compress.lzf.{LZFEncoder, LZFDecoder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.Utilities.EnumDelegate
import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes

import shark.SharkConfVars


/**
 * Java object serialization using XML encoder/decoder. Avoid using this to
 * serialize byte arrays because it is extremely inefficient.
 */
object XmlSerializer {
  // We prepend the buffer with a byte indicating whether payload is compressed
  val COMPRESSION_ENABLED: Byte = 1
  val COMPRESSION_DISABLED: Byte = 0

  def serialize[T](o: T, conf: Configuration): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val e = new XMLEncoder(byteStream)
    // workaround for java 1.5
    e.setPersistenceDelegate(classOf[ExpressionTypes], new EnumDelegate())
    e.setPersistenceDelegate(classOf[GroupByDesc.Mode], new EnumDelegate())
    // workaround for HiveConf-not-a-javabean
    e.setPersistenceDelegate(classOf[HiveConf], new HiveConfPersistenceDelegate )
    e.writeObject(o)
    e.close()

    val useCompression = conf match {
      case null => SharkConfVars.COMPRESS_QUERY_PLAN.defaultBoolVal
      case _ => SharkConfVars.getBoolVar(conf, SharkConfVars.COMPRESS_QUERY_PLAN)
    }

    if (useCompression) {
      COMPRESSION_ENABLED +: LZFEncoder.encode(byteStream.toByteArray())
    } else {
      COMPRESSION_DISABLED +: byteStream.toByteArray
    }
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    val cl = Thread.currentThread.getContextClassLoader
    val decodedStream =
      if (bytes(0) == COMPRESSION_ENABLED) {
        new ByteArrayInputStream(LZFDecoder.decode(bytes.slice(1, bytes.size)))
      } else {
        new ByteArrayInputStream(bytes.slice(1, bytes.size))
      }

    // Occasionally an object inspector is created from the decoding.
    // Need to put a lock on the process.
    val ret = {
      val d: XMLDecoder = new XMLDecoder(decodedStream, null, null, cl)
      classOf[XMLDecoder].synchronized {
        val ret = d.readObject()
        d.close()
        ret
      }
    }
    ret.asInstanceOf[T]
  }
}
