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

package shark.repl

import java.io.PrintWriter

import shark.{SharkContext, SharkEnv}

import spark.{SparkContext, SparkEnv}
import spark.repl.SparkILoop


/**
 * Add more Shark specific initializations.
 */
class SharkILoop extends SparkILoop(None, new PrintWriter(Console.out, true), None) {

  override def initializeSpark() {
    // Note: shark.SharkEnv.initWithSharkContext must be invoked after spark.repl.Main.interp
    // is used because the slaves' executors depend on the environmental variable
    // "spark.repl.class.uri" set to invoke Spark's ExecutorClassLoader.
    intp.beQuietDuring {
      command("""
        spark.repl.Main.interp.out.println("Creating SparkContext...");
        spark.repl.Main.interp.out.flush();
        shark.SharkEnv.initWithSharkContext("sharkJob")
        @transient val sc = shark.SharkEnv.sc.asInstanceOf[shark.SharkContext]
        @transient val sparkContext = sc
        spark.repl.Main.interp.out.println("Shark context available as sc.");
        import sc._;
        def s = sql2console _;
        spark.repl.Main.interp.out.flush();
        """)
      command("import spark.SparkContext._");
    }
    Console.println("Type in expressions to have them evaluated.")
    Console.println("Type :help for more information.")
    Console.flush()
  }
}

