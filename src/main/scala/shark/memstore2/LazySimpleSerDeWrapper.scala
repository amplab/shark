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

package shark.memstore2

import java.util.{List => JList, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.{SerDe, SerDeStats}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Writable


class LazySimpleSerDeWrapper extends SerDe {

  val _lazySimpleSerDe = new LazySimpleSerDe()

  override def initialize(conf: Configuration, tbl: Properties) {
  	_lazySimpleSerDe.initialize(conf, tbl)
  }

  override def deserialize(blob: Writable): Object = _lazySimpleSerDe.deserialize(blob)

  override def getSerDeStats(): SerDeStats = _lazySimpleSerDe.getSerDeStats()

  override def getObjectInspector: ObjectInspector = _lazySimpleSerDe.getObjectInspector

  override def getSerializedClass: Class[_ <: Writable] = _lazySimpleSerDe.getSerializedClass

  override def serialize(obj: Object, objInspector: ObjectInspector): Writable = {
  	_lazySimpleSerDe.serialize(obj, objInspector)
  }

}
