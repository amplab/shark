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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import com.ning.compress.lzf.LZFEncoder
import com.ning.compress.lzf.LZFDecoder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.io.Text


object HiveConfSerializer {

  def serialize(hConf: HiveConf): Array[Byte] = {
    val os = new ByteArrayOutputStream
    val dos = new DataOutputStream(os)
    val auxJars = hConf.getAuxJars()
    Text.writeString(dos, if(auxJars == null) "" else auxJars)
    hConf.write(dos)
    LZFEncoder.encode(os.toByteArray())
  }

  def deserialize(b: Array[Byte]): HiveConf = {
    val is = new ByteArrayInputStream(LZFDecoder.decode(b))
    val dis = new DataInputStream(is)
    val auxJars = Text.readString(dis)
    val conf = new HiveConf
    conf.readFields(dis)
    if(auxJars.equals("").unary_!)
      conf.setAuxJars(auxJars)
    conf
  }
}
