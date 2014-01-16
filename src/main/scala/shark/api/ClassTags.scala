package shark.api

import scala.reflect.classTag

object ClassTags {
  // List of primitive ClassTags.
  val jBoolean = classTag[java.lang.Boolean]
  val jByte = classTag[java.lang.Byte]
  val jShort = classTag[java.lang.Short]
  val jInt = classTag[java.lang.Integer]
  val jLong = classTag[java.lang.Long]
  val jFloat = classTag[java.lang.Float]
  val jDouble = classTag[java.lang.Double]
}
