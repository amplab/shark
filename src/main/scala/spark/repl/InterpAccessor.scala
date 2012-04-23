package spark.repl


/**
 * This is a hack so we can gain write access to spark.repl.Main.interp.
 * The variable is unfortunately only writable within spark.repl package.
 * Another option is to use Java reflection, but I am concerned about
 * using reflection to change code in Scala.
 */
object InterpAccessor {

  def interp = spark.repl.Main.interp

  def interp_=(i: SparkILoop) { spark.repl.Main.interp = i }

}

