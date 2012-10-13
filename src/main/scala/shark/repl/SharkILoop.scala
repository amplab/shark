package shark.repl

import java.io.PrintWriter
import shark.{SharkContext, SharkEnv}
import spark.SparkContext
import spark.repl.SparkILoop

/**
 * Add more Shark specific initializations.
 */
class SharkILoop extends SparkILoop(None, new PrintWriter(Console.out, true), None) {

  // Forces initializing the Kryo register. If we don't make the call explicit,
  // there is no guarantee that SharkEnv's static section is executed before
  // the Kryo serializer is initialized by Spark.
  SharkEnv.init()

  override def initializeSpark() {
    intp.beQuietDuring {
      command("""
        spark.repl.Main.interp.out.println("Creating SparkContext...");
        spark.repl.Main.interp.out.flush();
        @transient val sparkContext = SharkEnv.sc;
        @transient val sc = sparkContext.asInstanceOf[shark.SharkContext];
        spark.repl.Main.interp.out.println("Shark context available as sc.");
        import sc._;
        def s = sql2console _;
        spark.repl.Main.interp.out.flush();
        """)
      command("import spark.SparkContext._");
    }
    Console.println("Type in expressions to have them evaluated.")
    Console.println("Type :help for more information.")
  }
}
