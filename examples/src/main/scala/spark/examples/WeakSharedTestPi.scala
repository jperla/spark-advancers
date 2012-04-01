package spark.examples

import scala.math.random
import spark._
import SparkContext._

object WeakSharedTestPi {
  def main(args: Array[String]) {
    val iter = 40*1000*1000
    if (args.length == 0) {
      System.err.println("Usage: WeakSharedTestPi <host> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "WeakSharedTestPi")
    val slices = if (args.length > 1) args(1).toInt else 2
    

    var count = spark.accumulator(0)
    for (i <- spark.parallelize(1 to iter, slices)) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) {count += 1}
         
      //intln("Weak is jumping: " + WeakShared.ws.value+" count: "+i)
      /*
      WeakShared.ws.monotonicUpdate(new DoubleWeakSharable(i))
      if ( i % 1000 == 0 ) {
        WeakShared.sendWeakShared(WeakShared.ws)
      }
        */
    }
    //println("Weak is roughly " + WeakShared.ws.value)
    println("Pi is roughly " + 4 * count.value / iter.toDouble)
  }
}
