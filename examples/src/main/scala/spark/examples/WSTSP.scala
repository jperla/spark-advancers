package spark

import scala.math
import scala.math.random
import scala.io.Source
import scala.io.Source._
import scala.collection.mutable._
import scala.util.Random
import scala.collection.immutable._
import java.io._

/*
class TSPState (t: ArrayBuffer[Int] = ArrayBuffer.empty[Int], c: Double = -1) extends Serializable {
    var tour = t
    var cost = c
    
    override def toString(): String = {
        var s = new String()
        tour.foreach(p => s += p + ", ")
        return s
    }
}

object TSPStateAccumulatorParam extends AccumulatorParam[TSPState] {
    def addInPlace(t1: TSPState, t2: TSPState): TSPState = {
        if (t1.cost == -1)
            return t2
        if (t2.cost == -1)
            return t1

        if (t1.cost < t2.cost)
            return t1
        return t2
    }

    //Todo: this is weird. we dont touch initialValue
    def zero(initialValue: TSPState): TSPState = { return initialValue }
}

*/
object WSTSP {
  implicit object MinDoubleUpdatedProgressParam extends UpdatedProgressParam[Double] {
    def monotonicUpdate(old : UpdatedProgress[Double], newT: Double) : Boolean = 
	{
		if (newT < old.value) {
            println("old value was " + old.value);
			old.updateValue(newT)
            println("updated value to " + old.value);
            return true
        } else {
            return false
        }
	}
    def zero(initialValue: Double) = Double.PositiveInfinity
  }


    def randomCycle (size: Int, rand: Random): ArrayBuffer[Int] =  {
        var randNodes = ArrayBuffer.empty[Int]
        for(i <- 0 until size) {
            randNodes += i
        }
        return rand.shuffle(randNodes)
    }
    
    def perturbCycle (nodes: ArrayBuffer[Int], rand: Random) {
        val a = rand.nextInt(nodes.length)

        var temp = nodes(a)
        nodes(a) = nodes((a+1)%nodes.length)
        nodes((a+1)%nodes.length) = temp

    }

    def scoreCycle (nodes: ArrayBuffer[Int], points: ArrayBuffer[Array[Double]]): Double = {
        var distance : Double = 0
        for (i <- 0 until nodes.length){
            var p1 = points(nodes(i))
            var p2 = points(nodes((i+1)%nodes.length))
            distance += Math.sqrt((p1(1) - p2(1)) * (p1(1) - p2(1)) + (p1(0) - p2(0)) * (p1(0) - p2(0)))
        }
        return distance
    }
   
    
    
    def main(args: Array[String]) {
        if (args.length == 0) {
            System.err.println("Usage: regularTSP <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "RegularTSP")
        val slices = if (args.length > 2) args(2).toInt else 2
        val iter = if (args.length > 3) args(3).toInt else 100000
    
        /*
        val data  = ArrayBuffer.empty[Array[Double]]
        for(line <- Source.fromFile(args(1)).getLines()) {
            var city = new Array[Double](2)
            var node = line.split(' ')
            city(0) = node(1).toDouble
            city(1) = node(2).toDouble
            data += city
        }
       */
 
        //WeakShared.ws = new DoubleWeakSharable(Double.PositiveInfinity)
        var tour = sc.accumulator(new TSPState())(TSPStateAccumulatorParam)
        //data.foreach(p => p.foreach(q => println(q)))
        
        /*getting errors with broadcast variable even with their example. reading 
        everything in locally for now*/
        //var bdata = sc.broadcast(data)


        var bestTour = sc.updatedProgress(Double.PositiveInfinity)
        for (i <- sc.parallelize(1 to iter, slices)) {
            var rand = LocalRandom.getRandom()
            var shuffled = ArrayBuffer.empty[Int]
            
            /*
            if (tour.value.cost == WeakShared.ws.value){
                //throw new IOException("found an equal value")
                println("found equal value!!!")
                perturbCycle(LocalRandom.tempTour, rand)
                shuffled = LocalRandom.tempTour
            } else {
                shuffled = randomCycle(LocalRandom.tempTour.length, rand)
            }
            */
            shuffled = randomCycle(LocalRandom.tempTour.length, rand)
                
            var score = scoreCycle(shuffled, LocalRandom.getData())

            //WeakShared.ws.monotonicUpdate(new DoubleWeakSharable(score))
            bestTour.update(score)

            /*
            if ( i % 1000 == 0 ) {
                WeakShared.sendWeakShared(WeakShared.ws)
            }
            */

            LocalRandom.tempTour = shuffled

            var latestTour = new TSPState(shuffled, score)
            tour += latestTour
            var fromopt = score - 27603
            //println("Score is : "+score+" difference is : "+ fromopt +" best is : "+WeakShared.ws.value)
        }
        println("the final tour is: "+tour)
        println("the final score is: "+tour.value.cost)
        
    }
}    

    
