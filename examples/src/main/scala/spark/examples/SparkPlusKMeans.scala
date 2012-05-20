package spark.examples

import java.util.Random
import spark.SparkContext
import spark.SparkContext._
import spark.examples.Vector._
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap


import spark._


import spark.examples.Vector._


object LocalData {
  var centers = null : Map[Int, Array[(Int, Vector)]]
  var k = 0

  def mergeCenters(allcenters: Map[Int, Array[(Int, Vector)]]) : Array[(Int, Vector)] = {
    val k = LocalData.k
    // average up the centers in local storage
    var initial = new Array[(Int, Vector)](k)
    for (i <- 0 until k) {
        //#TODO: jperla: don't hardcode 2
        initial(i) = (0, Vector(2, _ => 0))
    }

    return allcenters.foldLeft(initial) {
        case (c, (chunk, centers)) => c.zip(centers).map{ case (a,b) => (a._1 + b._1, (b._2 * b._1) + a._2) }
    }.map {
        case (count, sum) => if (count > 0) { (count, sum / count) } else { (0, sum) }
    }
  }
}



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
        out = out + " converged: " + converged.toString
        return out
    }
}


class KMeansMapMessage (
    var chunk: Int, var centers: Array[(Int, Vector)]) extends Serializable
{
    override def toString: String = { 
        var out = KMeansLib.centersToString(centers)
        out = out + " chunk: " + chunk
        return out
    }
}


object KMeansProgress {
  type G = KMeansMapMessage
  type P = KMeansProgressUpdate
    
  class MasterMessage (
    var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P]
  {
    override def toString: String = {
        var out = new String
        out = out + "\n" + "id: " + id.toString + "\n"
        out = out + message //old: KMeansLib.centersToString(message)
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

    def zero(initialValue: P) : P = { 
        if (LocalData.centers == null) {
            // TODO: is this right?
            LocalData.centers = new HashMap[Int, Array[(Int, Vector)]]()
        }
        LocalData.k = initialValue.centers.length
        return new KMeansProgressUpdate(initialValue.centers, false)
    }

    def masterAggregate(oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {
        // save message in local storage
        LocalData.centers(message.chunk) = message.centers

        val newCenters = LocalData.mergeCenters(LocalData.centers)

        // save this average to oldVar
        oldVar.updateValue(new KMeansProgressUpdate(newCenters, false))
        //println("oldVar: " + oldVar)
    
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
      System.err.println("Usage: SparkPlusKMeans <master> <slices> <dimensions> <k> <iters> <file>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkKMeans")
    val slices = args(1).toInt
    val dimensions = args(2).toInt
    val k = args(3).toInt
    val iterations = args(4).toInt

    // Initialize cluster centers randomly
    val rand = new Random(42)
    var initialCenters = new Array[(Int, Vector)](k)
    for (i <- 0 until k) {
      initialCenters(i) = (0, Vector(dimensions, _ => 2 * rand.nextDouble - 1))
    }
    println("Initial centers: " + initialCenters.mkString(", "))

    //val allcenters = sc.updatedProgress(new KMeansProgressUpdate(new Array[(Int,Vector)](k), false), KMeansProgress.Modifier)
    val zero = new KMeansProgressUpdate(initialCenters, false)
    val allcenters = sc.updatedProgress(zero, KMeansProgress.Modifier)

    for (i <- sc.parallelize(0 until slices, slices)) {
      println("====== On chunk " + i)

      val filename = args(5) + i + ".txt"
      val lines = scala.io.Source.fromFile(filename).getLines
      val points = lines.map(parseVector _)

      //while (!allcenters.value.converged)
      for (j <- 0 until iterations) {
        val centers = allcenters.value.centers.clone()

        // Map each point to the index of its closest center and a (point, 1) pair
        // that we will use to compute an average later
        val mappedPoints = points.map { p => (closestCenter(p, centers.map{case (a,b) => b}), (p, 1)) }

        // Compute the new centers by summing the (point, 1) pairs and taking an average
        var initial = centers.map{x => (0, Vector(dimensions, _ => 0))}
        val newCenters = mappedPoints.foldLeft(initial) {
            // reduceByKey; vsum == vectorSum .
            case (c, (id, (vsum, count))) => c.update(id, (c(id)._1 + count, c(id)._2 + vsum)); c;
        }.map {
            case (count, vsum) => if (count > 0) { (count, vsum / count) } else { (0, vsum) }
        }

        // Update the centers array with the new centers we collected
        allcenters.advance(new KMeansMapMessage(i, newCenters))
      }
    }

    println("Final centers: " + allcenters.value.centers.mkString(", "))
    val finalCenters = LocalData.mergeCenters(LocalData.centers)
    println("Final centers: " + finalCenters.mkString(","))

    println("all centers:")
    println(LocalData.centers(0).mkString(","))
    println(LocalData.centers(1).mkString(","))
    println(LocalData.centers(2).mkString(","))
    println(LocalData.centers(3).mkString(","))
    println(LocalData.centers(4).mkString(","))
  }
}
