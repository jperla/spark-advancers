package spark

import spark._
import scala.io.Source
import scala.collection.mutable._
import java.util._

object LRHelpers {
    
    def parse(path: String, slices: Integer):Array[Array[Array[Double]]] = {
        val source = Source.fromFile(path)
        val lines = source.mkString.split("\n")
        source.close()

        val table = lines.map(l => l.split(",").map(i => i.toDouble))
	    val num_data = lines.length / slices
	    val num_dims = table(0).length
	    var index = 0
	
	    var retval = new Array[Array[Array[Double]]](slices)
	    for (i <- 0 until slices) {
	        retval(i) = new Array[Array[Double]](num_data)
	        for (j <- 0 until num_data) {
	    	    retval(i)(j) = new Array[Double](num_dims)
		
                for (k <- 0 until num_dims) {
		            retval(i)(j)(k) = table(index)(k)
		        }
		        index = index + 1
	        }
	    }         
	    return retval
    }

    def sigmoid(features: Array[Double], params: Array[Double]):Double = {
        var sum = 0.0
        for (i <- 0 until params.length){
            sum = sum - features(i+1)*params(i)
        }
        return 1/(1 + math.exp(sum))
    }

    def exampleGradient(f: Array[Array[Double]], params: Array[Double], startIndex: Integer, num: Integer) : Array[Double] = {
        var numFeatures = f(0).length - 1;
	    var numExamples = f.length 

	    var g = new Array[Double](numFeatures)
        var i = 0
        
        for (iter <- 0 until num) {
            i = (iter + startIndex) % numExamples
            val scale = (f(i)(0) - sigmoid(f(i), params))
            for (l <- 1 until f(i).length) {            
            	g(l-1) = g(l-1) + scale * f(i)(l)   
	        }
            
            /*
            if (iter == 0)
            {
                TicTocLR.appendToFile("TTLR.log", "first gradient: " + g(0) + "," + g(1))
       	    }

            */
        }
	    return g
     }

     def regularizedGradient(g: Array[Double], alpha: Double, params: Array[Double]) : Array[Double] = {
        var ret = new Array[Double](g.length)
	    for (i <- 0 until g.length) {
	        ret(i) = g(i) - 2 * alpha * params(i)
	    }
	    return ret
     }

     def gradient(f: Array[Array[Double]], params: Array[Double], startIndex: Integer, num: Integer, alpha: Double) : Array[Double] =  
     {
	    return regularizedGradient(exampleGradient(f, params, startIndex, num), alpha, params)
     }
}

object AsymUPConvex {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: AsymUPConvex <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "AsymUPConvex")
        val path = args(1)
        val slices = if (args.length > 2) args(2).toInt else 1
        
        val chunkSize = 1000
        val distFile = LRHelpers.parse(path, slices)
        val alpha = 0.0
   
        var buf = new Array[Double](distFile(0)(0).length - 1)
        var x = sc.updatedProgress(new LRProgressUpdate(buf, false), LRProgress.Modifier)        
    
        val start = System.currentTimeMillis()
        for (f <- sc.parallelize(distFile, slices)) {
            
            var cloneX = new Array[Double](f(0).length -1)
	        var startIndex = 0
            while (!x.value.converged) {
                //Note: synchronize
                // Is this needed?
	            for (k <- 0 until cloneX.length) {
                    cloneX(k) = x.value.position(k)
                }                		

		        //TicTocLR.appendToFile("TTLR.log", "start_index: " + startIndex)
                
                var g = LRHelpers.exampleGradient(f, cloneX, startIndex, chunkSize)
	    	    startIndex = (startIndex + chunkSize) % f.length

		        for (j <- 0 until g.length) {
		            g(j) = -g(j)
		        } 
                                
		        /*
		        TicTocLR.appendToFile("TTLR.log", "########################")
                for ( j <- 0 until g.length){
                    TicTocLR.appendToFile("TTLR.log", "g outside: " + g(j).toString)
                }
                TicTocLR.appendToFile("TTLR.log", "########################")
		        */

                x.advance(g)
       		}
        }
        
        val stop = System.currentTimeMillis()
        println ("Job length: " + ((stop - start)/1000).toString + " seconds")

        println("#########################")
        println("Final value of x: " + x.value)
        println("#########################")
        sc.stop()
    }
}
