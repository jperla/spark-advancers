package spark.examples

import java.util.Random
import spark.SparkContext
import spark.SparkContext._
import spark.examples.Vector._
import scala.collections.mutable.Map

object SparkPlusKMeans {
  def parseVector(line: String): Vector = {
    return new Vector(line.split(' ').map(_.toDouble))
  }

  def closestCenter(p: Vector, centers: Array[Vector]): Int = {
    var bestIndex = 0
    var bestDist = p.squaredDist(centers(0))
    for (i <- 1 until centers.length) {
      val dist = p.squaredDist(centers(i))
      if (dist < bestDist) {
        bestDist = dist
        bestIndex = i
      }
    }
    return bestIndex
  }




    class KMeansState (c: Map[Int, (Array[Int], Array[Vector])] = Map.empty[Int, (Array[Int], Array[Vector])]) extends Serializable {
        var centers = c
        
        override def toString(): String = {
            var s = new String("k means centers")
            //c.foreach(p => s += p + ", ")
            return s
        }
    }

    object KMeansStateAccumulatorParam extends AccumulatorParam[KMeansState] {
        def addInPlace(t1: KMeansState, t2: KMeansState): KMeansState = {
            for (key <- t2) {
                t1(key) = t2(key)
            }
            return t1
        }

        /*Todo: this is weird. we dont touch initialValue*/
        def zero(initialValue: KMeansState): KMeansState = { return initialValue }
    }




  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SparkPlusKMeans <master> <file> <dimensions> <k> <iters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkKMeans")
    val dimensions = args(2).toInt
    val k = args(3).toInt
    val iterations = args(4).toInt

    // Initialize cluster centers randomly
    val rand = new Random(42)
    var centers = new Array[Vector](k)
    for (i <- 0 until k) {
      centers(i) = Vector(dimensions, _ => 2 * rand.nextDouble - 1)
    }
    println("Initial centers: " + centers.mkString(", "))

    val allcenters = sc.updatedProgress(Map.empty, KMeansProgress.Modifier)

    for (i <- 1 to iterations) {
      println("On iteration " + i)

      val lines = sc.textFile(args(1) + i + ".txt")
      val points = lines.map(parseVector _)

      // Map each point to the index of its closest center and a (point, 1) pair
      // that we will use to compute an average later
      val mappedPoints = points.map { p => (closestCenter(p, centers), (p, 1)) }

      // Compute the new centers by summing the (point, 1) pairs and taking an average
      val newCenters = mappedPoints.reduceByKey {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }.map { 
        case (id, (sum, count)) => (id, sum / count)
      }.collect

      // Update the centers array with the new centers we collected
      for ((id, value) <- newCenters) {
        centers(id) = value
      }
    }

    println("Final centers: " + centers.mkString(", "))
  }
}
