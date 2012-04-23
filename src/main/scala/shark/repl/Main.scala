package shark.repl

// Note that InterpAccessor is a hack to get write access to spark.repl.Main.interp.
import spark.repl.InterpAccessor
import spark.repl.SparkILoop


/**
 * Shark's REPL entry point.
 */
object Main {
  
  private var _interp: SharkILoop = null
  
  def interp = _interp
  
  private def interp_=(i: SharkILoop) { _interp = i }
  
  def main(args: Array[String]) {

    _interp = new SharkILoop

    // We need to set spark.repl.InterpAccessor.interp since it is used
    // everywhere in spark.repl code.
    spark.repl.InterpAccessor.interp = _interp

    // Start an infinite loop ...
    _interp.process(args)
  }
}
