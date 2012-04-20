package spark

import spark._
import scala.io.Source
import scala.collection.mutable._
import java.util._

object ObjectiveLR {
    

    def parseParams(path: String): Array[Array[Double]] = {
        val source = Source.fromFile(path)
        val lines = source.mkString.split("\n")
        source.close()

        val table = lines.map(l => l.split(",").map(i => i.toDouble))

        return table
    }
        
    def exponent(features: Array[Double], params: Array[Double]):Double = {
        var sum = 0.0
        for (i <- 0 until params.length){
            sum = sum - features(i+1) * params(i)
        }
        return sum
    }

    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: ObjectiveLR <host> <data> <params>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "ObjectiveLR")
        val dataPath = args(1)
        val paramsPath = args(2)
        val slices = 1
        //val slices = if (args.length > 2) args(2).toInt else 1
        //val iter = if (args.length > 3) args(3).toInt else 10000

        //val chunkSize = 200
        val distFile = LRHelpers.parse(dataPath, slices)
        val data = distFile(0)
        System.out.println("num of examples: " + data.length.toString)
        //val numExamples = LRHelpers.getNumExamples(dataPath)
        val params = parseParams(paramsPath)
        System.out.println("num of params: " + params.length.toString)
        //val alpha = 0.1
        //var buf = new Array[Double](distFile(0)(0).length - 1)
        //val tic = new Date().getTime()
        //var x = sc.updatedProgress(new LRProgressUpdate(buf, false, 0, tic), LRProgress.Modifier)        
        //val runtime = 1000
        var objectives = new Array[Double](params.length)

        for (i <- 0 until params.length){

            var objective = 0.0
            for (j <- 0 until data.length){
                val h = exponent(data(j), params(i))
                val response = data(j)(0)
                if (h >= 400){
                    val t = (-h * (response - 1)) - h
                    objective = objective + t
                } else{
                    val tail = (-h * (response - 1)) - math.log(1 + math.exp(h))
                    objective = objective + tail
                }
                //System.out.println(h)
            }
            objectives(i) = objective
        }

        for (k <- 0 until objectives.length) {
            TicTocLR.appendToFile ("Objectives.log", objectives(k).toString)
        }
        
    }
}
