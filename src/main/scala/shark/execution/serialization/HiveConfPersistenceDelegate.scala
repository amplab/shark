/*
* Copyright (c) Clear Story Data, Inc. All Rights Reserved.
*
* Please see the COPYRIGHT file in the root of this repository for more
* details.
*/
package shark.execution.serialization

import java.beans.{Statement, Encoder, DefaultPersistenceDelegate}
import org.apache.hadoop.hive.conf.HiveConf
import scala.collection.JavaConversions._
import org.apache.commons.lang.ObjectUtils

class HiveConfPersistenceDelegate extends DefaultPersistenceDelegate {
  override protected def initialize(clazz: Class[_], oldInst: AnyRef, newInst: AnyRef, out: Encoder) {
    val oldConf = oldInst.asInstanceOf[HiveConf]
    val newConf = newInst.asInstanceOf[HiveConf]

    if (!ObjectUtils.equals(oldConf.getAuxJars, newConf.getAuxJars))
      out.writeStatement(new Statement(oldInst, "setAuxJars", Array(oldConf.getAuxJars)))

    val oldConfProps = oldConf.getAllProperties
    val newConfProps = newConf.getAllProperties

    val propsToDelete = newConfProps.filter { case(k, v) => !oldConfProps.containsKey(k) }
    val propsToAdd = oldConf.getAllProperties.filter { case(k, v) =>
      !newConfProps.containsKey(k) || !ObjectUtils.equals(newConfProps.get(k), v)
    }

    propsToDelete.foreach { case(k, v) =>
      out.writeStatement(new Statement(oldInst, "unset", Array(k)))
    }
    propsToAdd.foreach {  case(k, v) =>
      out.writeStatement(new Statement(oldInst, "set", Array(k, v)))
    }
  }
}
