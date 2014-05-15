
/**
 * Shark is a Hive compatible query execution system based on Spark SQL.
 */
package object shark {
  def Logger(name: String) =
    com.typesafe.scalalogging.slf4j.Logger(org.slf4j.LoggerFactory.getLogger(name))

  type Logging = com.typesafe.scalalogging.slf4j.Logging
}