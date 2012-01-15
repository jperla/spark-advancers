package spark

import spark._
import scala.io.Source
import scala.collection.mutable._

object AsymUPConvex {
    
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

    def sigmoid(feature: Array[Double], x: Array[Double]):Double = {
        var sum = 0.0
        for (i <- 0 until x.length){
            sum = sum - x(i)*feature(i+1)
        }
           
        return 1/(1 + math.exp(sum))
    }          

    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: AsymUPConvex <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "AsymUPConvex")
        val path = args(1)
        val slices = if (args.length > 2) args(2).toInt else 2
        val iter = if (args.length > 3) args(3).toInt else 100000
        val chunkSize = 7000
        val distFile = parse(path, slices)
        val N = distFile.length
   
        println("slices: ",slices)
        var buf = new Array[Double](distFile(0)(0).length - 1)
        var x = sc.updatedProgress(new LRProgressUpdate(buf, false), LRProgress.Modifier)        
    
        for (f <- sc.parallelize(distFile, slices)) {
            val totalChunks = f.length/chunkSize
            val remainder = f.length%(chunkSize)
            var cloneX = new Array[Double](f(0).length -1)

            while (!x.value.converged) {
                //Note: synchronize
                var i = 0
                var g = new Array[Double](f(0).length - 1)
                
                println("CloneX: ")
                for (k <- 0 until cloneX.length){
                    cloneX(k) = x.value.position(k)
                    println(cloneX(k))
                } 
                
                while (i < totalChunks*chunkSize){
                    
                    if (i > 0 && (i % chunkSize == 0)){
                        
                        x.advance(g)
                        println("CloneX: ")
                        for (k <- 0 until cloneX.length){
                            cloneX(k) = x.value.position(k)
                            println(cloneX(k))
                        }
                        g = new Array[Double](f(0).length - 1)
                    }
                        
                    val scale = (f(i)(0) - sigmoid(f(i), cloneX))
                    for (l <- 1 until f(i).length) {
                        g(l-1) = g(l-1) + scale * f(i)(l)   
                    }
                    i += 1
                }
                x.advance(g)
                Thread.sleep(10000)

                if (f.length != i)
                {
                    for (k <- 0 until cloneX.length){
                        cloneX(k) = x.value.position(k)
                    }                
                
                    g = new Array[Double](f(0).length - 1)
                
                    for(example <- i until f.length) {
                        val scale = (f(example)(0) - sigmoid(f(example), cloneX))

                        for (l <- 1 until f(example).length) {
                            g(l-1) = g(l-1) + scale * f(example)(l)
                        }
                    }
                    x.advance(g)
                }
            }
        }
        println("Final value of x: ", x.value)
    }
}
