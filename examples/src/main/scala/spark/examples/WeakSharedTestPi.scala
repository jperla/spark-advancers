package spark.examples

import scala.math.random
import spark._
import SparkContext._

object WeakSharedTestPi {
  def main(args: Array[String]) {
    val iter = 1000000
    if (args.length == 0) {
      System.err.println("Usage: WeakSharedTestPi <host> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "WeakSharedTestPi")
    val slices = if (args.length > 1) args(1).toInt else 2
    
	var weak = new DoubleWeakSharable(0.0)

    var count = spark.accumulator(0)
    for (i <- spark.parallelize(1 to iter, slices)) {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) {count += 1}
      
      weak.monotonicUpdate(new DoubleWeakSharable(i))
      if(i%10 == 0)
        Accumulators.sendWeakShared(weak)

    }
    println("Weak is roughly " + weak.value)
    println("Pi is roughly " + 4 * count.value / iter.toDouble)
  }
}
