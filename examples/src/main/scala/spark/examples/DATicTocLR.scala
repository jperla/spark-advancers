package spark

import spark._
import scala.io.Source
import scala.collection.mutable._
import java.io._
import scala.util.Random
import java.util.Date


object DATicTocLR {
   
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: DATicTocLR <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "DATicTocLR")
        val path = args(1)
        val slices = if (args.length > 2) args(2).toInt else 1
        val iter = if (args.length > 3) args(3).toInt else 10000
        val chunkSize = 200
        
        val distFile = LRHelpers.parse(path, slices)
        val numExamples = LRHelpers.getNumExamples(path)
	    val numFeatures = distFile(0)(0).length - 1
        var buf = new Array[Double](numFeatures)
        var x = new Array[Double](numFeatures)
	    var z = new Array[Double](numFeatures)
        var g = Array[Double](numFeatures)
        val grad = sc.accumulator(buf)(ArrayDoubleAccumulatorParam)
        val alpha = 1.0
        val epsilon = 1e-4
        var converged = false
        var c = 1
        val tic = new Date().getTime()
        var period = new Date().getTime()
        val runtime = 500
        var change = 0.0

        while(!converged) 
	    {  
            for (f <- sc.parallelize(distFile, slices)) 
	        {
                val rand = new Random(new Date().getTime())
                val start = rand.nextInt(f.length)
	            grad += LRHelpers.exampleGradient(f, x, start, chunkSize)
       	    }
            
/*
            appendToFile("TTLR.log", "########################")
            for ( j <- 0 until g.length){
                appendToFile("TTLR.log", "g outside: " + g(j).toString)
            }
            appendToFile("TTLR.log", "########################")
*/

            change = 0.0
            for (i <- 0 until numFeatures)
	        {
		        z(i) = z(i) - grad.value(i) * numExamples / chunkSize / slices
		        grad.value(i) = 0
		        var newValue = -z(i) / 2 / c 
		        change = (newValue - x(i)) * (newValue - x(i))
		        x(i) = newValue
 	        }
	        
            val check = new Date().getTime()
            if ((check - period)/1000 >= 1.0) {

                TicTocLR.appendToFile("DATTLRchange.log", change.toString)
                var xstring = x(0).toString
                for ( j <- 1 until x.length){
                    xstring += ("," + x(j).toString)
                }
                TicTocLR.appendToFile("DATTLRparam.log", xstring)

                period = new Date().getTime()
            }
            
            if (math.sqrt(change) < epsilon || ((check - tic)/1000 > runtime)) { converged = true }
            c += 1
    	}

        val toc = new Date().getTime()
        
        TicTocLR.appendToFile("DATTLR.log", "DATicTocLR Running Time: " + ((toc - tic)/1000).toString)
        TicTocLR.appendToFile("DATTLR.log", "DATicTocLR change: " + change.toString)
        for (j <- 0 until x.length){
            TicTocLR.appendToFile("DATTLR.log", x(j).toString)
        }

	    sc.stop()
    }
}


