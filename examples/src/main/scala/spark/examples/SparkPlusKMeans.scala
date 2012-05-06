package spark.examples

import java.util.Random
import spark.SparkContext
import spark.SparkContext._
import spark.examples.Vector._
import scala.collection.mutable.Map



import spark._


import spark.examples.Vector._

object KMeansLib {
    def centersToString(centers: Array[(Int, Vector)]): String = {
        var out = new String
        for (c <- centers){
            out = out + "N: " + c._1 + ", center: " + c._2.toString + "\n"
        }
        return out
    }
}

class KMeansProgressUpdate (
    var centers: Array[(Int, Vector)], var converged: Boolean) extends Serializable
{
    override def toString: String = { 
        var out = KMeansLib.centersToString(centers)
        out = out + "converged: " + converged.toString
        return out
    }
}

object KMeansProgress {
  type G = Array[(Int, Vector)]
  type P = KMeansProgressUpdate
    
    
  class MasterMessage (
    var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P]
  {
    override def toString: String = {
        var out = new String
        out = out + "\n" + "id: " + id.toString + "\n"
        out = out + KMeansLib.centersToString(message)
        return out
    }
  }

  class Diff (
    var id:Long, @transient message: G, @transient theType: P) extends UpdatedProgressDiff[G,P]
  {
    var myValue = theType
    def update(oldVar : UpdatedProgress[G,P]) = {
        // todo: locking?
        oldVar.updateValue(myValue)
    }

    override def toString = "\n" + "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[G,P] {
    val eps = 1e-10

    def updateLocalDecideSend(oldVar: UpdatedProgress[G,P], message: G) : Boolean = {
        // always send G; no change to local state
        return true
	}

    def zero(initialValue: P) = new KMeansProgressUpdate(new Array[(Int, Vector)](initialValue.centers.size), false)

    def masterAggregate(oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {
        var diff = new Diff(oldVar.id, message, oldVar.value)
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressMasterMessage[G,P] = {
        return new MasterMessage(oldVar.id, message, 
            new KMeansProgressUpdate(new Array[(Int,Vector)](0), false))
    }
  }
}








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

    val allcenters = sc.updatedProgress(new KMeansProgressUpdate(new Array[(Int,Vector)](k), false), KMeansProgress.Modifier)

    for (i <- 0 to iterations) {
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
      }

      // Update the centers array with the new centers we collected
      for ((id, value) <- newCenters) {
        centers(id) = value
      }
    }

    println("Final centers: " + centers.mkString(", "))
  }
}
