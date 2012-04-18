package spark

import spark._
import scala.io.Source
import scala.collection.mutable._
import java.io._
import scala.util.Random
import java.util.Date

object ArrayDoubleAccumulatorParam extends AccumulatorParam[Array[Double]] {
    def addInPlace(t1: Array[Double], t2: Array[Double]): Array[Double] = {
        var result = new Array[Double](t1.length)
        for(i <- 0 until result.length){
            result(i) = t1(i) + t2(i)
        }
        return result
    }

    def zero(initialValue: Array[Double]): Array[Double] = {
        var z = new Array[Double](initialValue.length)
        for(i <- 0 until z.length){
            z(i) = 0
        }
        return z
    }
}


object TicTocLR {
   
    def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
        try { f(param) } finally { param.close() }

    def writeToFile(fileName:String, data:String) =
          using (new FileWriter(fileName)) {
                    fileWriter => fileWriter.write(data)
    }

    def appendToFile(fileName:String, textData:String) =
      using (new FileWriter(fileName, true)){
        fileWriter => using (new PrintWriter(fileWriter)) {
          printWriter => printWriter.println(textData)
        }
      }

    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: TicTocLR <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "TicTocLR")
        val path = args(1)
        val slices = if (args.length > 2) args(2).toInt else 1
        val iter = if (args.length > 3) args(3).toInt else 10000
        //val chunkSize = 400
        
        val distFile = LRHelpers.parse(path, slices)
        val numFeatures = distFile(0)(0).length - 1
        var buf = new Array[Double](numFeatures)
        var x = new Array[Double](numFeatures)
        var g = Array[Double](numFeatures)
        val grad = sc.accumulator(buf)(ArrayDoubleAccumulatorParam)
        val alpha = 1.0
        val epsilon = 0.0001
        var converged = false
        var c = 1
        val tic = new Date().getTime()
        var period = new Date().getTime()
        val runtime = 1000

        while(!converged) 
	    {  
            for (f <- sc.parallelize(distFile, slices)) 
	        {
                val rand = new Random(new Date().getTime())
                val start = rand.nextInt(f.length)
                val numExamples = f.length
	            grad += LRHelpers.exampleGradient(f, x, start, numExamples)
       	    }
            
            g = LRHelpers.regularizedGradient(grad.value, alpha, x)
            
            for ( j <- 0 until grad.value.length){
                grad.value(j) = 0.0
            }

/*
            appendToFile("TTLR.log", "########################")
            for ( j <- 0 until g.length){
                appendToFile("TTLR.log", "g outside: " + g(j).toString)
            }
            appendToFile("TTLR.log", "########################")
*/

            var change = 0.0
            for (i <- 0 until numFeatures)
	        {
		        x(i) = x(i) + 1.0/c * g(i)
            	change = 1.0/c/c * g(i) * g(i) + change
 	        }
	        
            val check = new Date().getTime()
            if ((check - period)/1000 >= 10){

                val timex = (check - tic)/1000
                appendToFile("TTLRchange.log", change.toString)
                var xstring = x(0).toString
                for ( j <- 1 until x.length){
                    xstring += ("," + x(j).toString)
                }
                appendToFile("TTLRparam.log", xstring)

                period = new Date().getTime()
            }
            
            if (math.sqrt(change) < epsilon || ((check - tic)/1000 > runtime)) { converged = true }
            c += 1
    	}

        val toc = new Date().getTime()
        
        appendToFile("TTLR.log", "TicTocLR Running Time: " + ((toc - tic)/1000).toString)
        for ( j <- 0 until x.length){
            appendToFile("TTLR.log", x(j).toString)
        }

	    sc.stop()
    }
}


