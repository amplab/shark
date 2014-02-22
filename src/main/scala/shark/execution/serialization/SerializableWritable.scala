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

import java.io._
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.NullWritable

object SerializableWritable {
  val conf = new JobConf()
}


class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {
  def value = t

  override def toString = if(null == t) "null" else t.toString

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    new ObjectWritable(if (t == null) NullWritable.get() else t).write(out)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(SerializableWritable.conf)
    ow.readFields(in)
    val s = ow.get
    if (s == null || s.isInstanceOf[NullWritable]) {
      t = null.asInstanceOf[T]
    } else {
      t = s.asInstanceOf[T]
    }
  }

  override def hashCode(): Int = if(t == null) 0 else t.hashCode

  override def equals(other: Any) = {
    if(other.isInstanceOf[SerializableWritable[_]].unary_!) {
      false
    } else {
      val other_t = other.asInstanceOf[SerializableWritable[_]].t
      if (t == null) {
        other_t == null
      } else {
        t.equals(other_t)
      }
    }
  }
}
