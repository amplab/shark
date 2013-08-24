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

import java.beans.{Statement, Encoder, DefaultPersistenceDelegate}
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.commons.lang.ObjectUtils

class HiveConfPersistenceDelegate extends DefaultPersistenceDelegate {
  override protected def initialize(clazz: Class[_], oldInst: AnyRef, newInst: AnyRef, out: Encoder)
  {
    val oldConf = oldInst.asInstanceOf[HiveConf]
    val newConf = newInst.asInstanceOf[HiveConf]

    if (!ObjectUtils.equals(oldConf.getAuxJars, newConf.getAuxJars)) {
      out.writeStatement(new Statement(oldInst, "setAuxJars", Array(oldConf.getAuxJars)))
    }

    val oldConfProps = oldConf.getAllProperties
    val newConfProps = newConf.getAllProperties

    val propsToDelete = newConfProps.filter { case(k, v) => !oldConfProps.containsKey(k) }
    val propsToAdd = oldConf.getAllProperties.filter { case(k, v) =>
      !newConfProps.containsKey(k) || !ObjectUtils.equals(newConfProps.get(k), v)
    }

    propsToDelete.foreach { case(k, v) =>
      out.writeStatement(new Statement(oldInst, "unset", Array(k)))
    }
    propsToAdd.foreach { case(k, v) =>
      out.writeStatement(new Statement(oldInst, "set", Array(k, v)))
    }
  }
}
