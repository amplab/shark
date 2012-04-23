package shark.parse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.Context

/**
 * Shark's query context. Adds Shark-specific information to Hive's Context.
 */
class QueryContext(
  conf: Configuration,
  val useTableRddSink: Boolean)
extends Context(conf)
