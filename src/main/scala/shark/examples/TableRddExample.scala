package shark.examples

import shark._
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector


object TableRddExample {

  def main(args: Array[String]) {
    
    val sc = new SharkContext(
      if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
      "Shark::" + java.net.InetAddress.getLocalHost.getHostName)
    
    SharkEnv.sc = sc
    
    //sc.sql2console("select count(*) from pokes")
    
    val rdd = sc.sql2rdd("select * from tttttt")
    println(rdd.prev.count())
    println(rdd.count())
    
    println(rdd.mapRows(_.getInt(0)).reduce(_+_))
    
    println(rdd.mapRows(_.getInt(0) + 1).reduce(_+_))
    
    println(rdd.mapRows(_.getInt("foo") + 1).reduce(_+_))
    
  }

}