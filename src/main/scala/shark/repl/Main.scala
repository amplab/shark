package shark.repl

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
    spark.repl.Main.interp = _interp

    // Start an infinite loop ...
    _interp.process(args)
  }
}
