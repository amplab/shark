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

import org.apache.commons.beanutils.PropertyUtils


/**
 * This is the bean utility for set/retrieve value from the cg row.
 */
object BeanPropertyHelper {
  def getField(ref: Any, propertyName: String) = {
    val field = ref.getClass.getDeclaredField(propertyName)
    if (!field.isAccessible()) field.setAccessible(true)

    field
  }

  def getMethod(ref: Any, methodName: String, args: Class[_]*) = {
    val method = ref.getClass.getMethod(methodName, args: _*)
    if (!method.isAccessible()) method.setAccessible(true)

    method
  }

  def instantiate(className: String): AnyRef = {
    val clazz = Thread.currentThread.getContextClassLoader().loadClass(className)
    try {
      clazz.newInstance().asInstanceOf[AnyRef]
    } catch {
      case x: Throwable => {
        x.printStackTrace()
        throw new RuntimeException(x)
      }
    }
  }

  def simpleCall(ref: Any, methodName: String, args: AnyRef*) = {
    val method = getMethod(ref, methodName, args.map(x => {
      x match {
        case _: java.lang.Integer => Integer.TYPE
        case _: java.lang.Byte    => java.lang.Byte.TYPE
        case _: java.lang.Short   => java.lang.Short.TYPE
        case _: java.lang.Float   => java.lang.Float.TYPE
        case _: java.lang.Long    => java.lang.Long.TYPE
        case _: java.lang.Double  => java.lang.Double.TYPE
        case _: java.lang.Boolean => java.lang.Boolean.TYPE
        case _                    => x.getClass()
      }
    }): _*)

    method.invoke(ref, args: _*)
  }

  def setPropertyValue(ref: Any, name: String, value: Any) {
    getField(ref, name).set(ref, value)
  }

  def getPropertyValue(ref: Any, name: String) = {
    getField(ref, name).get(ref)
  }

  def setPropertyValueBySetterMethod(ref: Any, name: String, value: Any) {
    PropertyUtils.setProperty(ref, name, value)
  }

  def getPropertyValueByGetterMethod(ref: Any, name: String) = {
    PropertyUtils.getProperty(ref, name)
  }
}