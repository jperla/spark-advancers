package spark

import spark._
import SparkContext._
import scala.io.Source
import scala.collection.mutable._
import scala.util.Random
import java.util.Date
import java.util.ArrayList
import org.apache.commons.math.special.Gamma._
import org.apache.commons.math._
import org.apache.commons.math.random._
import java.io.File
import java.io.IOException
import java.io.ObjectInputStream

// Most of this code would be cleaner with the introduction of a vector class with abstractions like exp, log, inner_product
class PDocument extends Serializable {
      var counts = new ArrayList[Double]();
      var ids = new ArrayList[Int]();
      var num_words = 0
}

class PoissonExample extends Serializable { 
      var doc = new PDocument();
      var label = 0.0
}

// The last weight is the intercept term
class PoissonWeights(val num_features:Int) extends Serializable {
      var weights = new Array[Double](num_features)

      override def toString() : String  = {
	  var result = ""
	  for (f <- 0 until num_features) {
	      result = result + weights(f).toString + ","
	  }
	  result = result.substring(0, result.length - 1) +  "\n"
	  return result
      }

      def random_init() {
      	  var rng = new RandomDataImpl()
	  rng.reSeed()
	  
	  val sigma = .1
      	  for (i <- - 0 until num_features) {
	      weights(i) = rng.nextGaussian(0, sigma)
	  }
      }

      override def clone() : PoissonWeights = {
      	  var retval = new PoissonWeights(num_features)
	  System.arraycopy(weights, 0, retval.weights, 0, num_features)
	  return retval
      }
}

class PoissonGradient(val num_features: Int) extends Serializable {
      var grad = new Array[Double](num_features)

      def clear() {
      	  for (i <- 0 until num_features) {
	      grad(i) = 0
	  }
      }
 
     override def clone() : PoissonGradient = {
      	  var retval = new PoissonGradient(num_features)
	  System.arraycopy(grad, 0, retval.grad, 0, num_features)
	  return retval
      }

      override def toString() : String  = {
	  var result = ""
	  for (f <- 0 until num_features) {
	      result = result + grad(f).toString + ","
	  }
	  result = result.substring(0, result.length - 1) +  "\n"
	  return result
      }
}

class PoissonDuals(val num_features: Int) extends Serializable {
      var duals = new Array[Double](num_features)
}

object PoissonRegression {

       def objective(examples: Array[PoissonExample], weights: PoissonWeights, indices: Array[Int], alpha: Double) : Double = {
	   var objective = 0.0
	   for (i <- 0 until indices.length) {
	       val index = indices(i)
	       val example = examples(index)
	       val ip = inner_product(example, weights)
	       var update = 0.0
	       if (ip > 10) {
	       	 update = -math.exp(10)
	       } else {
	       	 update = ip * example.label - math.exp(ip)
	       }
	       
	       objective += update
	    
	   }

	   for (i <- 0 until weights.num_features) {
	       objective = objective + alpha * weights.weights(i) * weights.weights(i)
	   }
	   //TicTocLR.appendToFile("DEBUG", "Objective: " + objective.toString)
	   return objective
       }

       def update_weight(weights: PoissonWeights, duals: PoissonDuals, grad: PoissonGradient, learning_rate: Double, num_iter: Int) : Double = {
       	   var change = 0.0
       	   for (i <- 0 until weights.num_features) {
	       duals.duals(i) = duals.duals(i) + grad.grad(i)
	       var update = -learning_rate * duals.duals(i) // RAJESH: Check the minus sign 
	       var new_weight = update //(num_iter - 1) * weights.weights(i) / num_iter + update / num_iter
	       change += math.abs(new_weight - weights.weights(i))
	       weights.weights(i) = new_weight
	   }
	   return change
       }

       def inner_product(example: PoissonExample, weights:PoissonWeights) : Double = {
       	   var retval = 0.0
       	   for (w <- 0 until example.doc.ids.size()) {
	       val index = example.doc.ids.get(w)
	       retval += weights.weights(index) * example.doc.counts.get(w)
	   }
	   retval += weights.weights(weights.num_features - 1)
	   return retval
       }

       def prediction(example: PoissonExample, weights:PoissonWeights) : Double = {
       	   return math.exp(inner_product(example, weights))
       }

       def safe_prediction_error(example: PoissonExample, weights:PoissonWeights) : Double = {
       	   var ip = inner_product(example, weights)
	   if (ip > 10) {
	       ip = 10
	   } 
	   return math.exp(ip) - example.label
       }

       def example_gradient(example: PoissonExample, weights:PoissonWeights, grad: PoissonGradient) {
       	   val prediction_error = safe_prediction_error(example, weights)
	   for (w <- 0 until example.doc.ids.size()) {
	       val index = example.doc.ids.get(w)
	       grad.grad(index) += prediction_error *  example.doc.counts.get(w)
	       if (grad.grad(index) == Double.PositiveInfinity) {
	       	  TicTocLR.appendToFile("DEBUG", "NAN FOUND: " + prediction_error)
	       }
	   }
	   grad.grad(weights.num_features - 1) += prediction_error
       }

       def scale_gradient(grad: PoissonGradient, num_examples: Int, chunk_size: Int, weights: PoissonWeights, alpha: Double) {
       	   for (i <- 0 until grad.grad.length) {
	       grad.grad(i) = num_examples * grad.grad(i) / chunk_size
      	       grad.grad(i) = grad.grad(i) + alpha * 2 * weights.weights(i)
	   }
       } 

       def gradient(examples: Array[PoissonExample], weights: PoissonWeights, indices: Array[Int], grad: PoissonGradient, alpha:Double) {
       	   for (i <- 0 until indices.length) {
	       val index = indices(i)
	       val example = examples(index)
	       example_gradient(example, weights, grad)
	   }
	   
	   scale_gradient(grad, examples.length, indices.length, weights, alpha)
       }

       // Needed for the tic-toc implementation
       def no_scale_gradient(examples: Array[PoissonExample], weights: PoissonWeights, indices: Array[Int], grad: PoissonGradient) {
       	   for (i <- 0 until indices.length) {
	       val index = indices(i)
	       val example = examples(index)
	       example_gradient(example, weights, grad)
	   }
       }
}

object PoissonDataReader {
       def read(path: String, id: String) : Array[PoissonExample] = {
       	   val doc_path : String = path + "/" + id
	   var examples = new Array[PoissonExample](get_num_examples(doc_path))
	   var index = 0
	   var max_id = 0;
	   for (line <- scala.io.Source.fromFile(new File(doc_path)).getLines()) {
	       examples(index) = new PoissonExample()
	       val tokens = line.split(" ");
	       
	       // Check and see if the label is missing
	       var label_missing = false
	       try {
	       	   examples(index).label = tokens(0).toInt
	       } catch {
	       	 case ne: NumberFormatException => label_missing = true
	       }

	       for (i <- 1 until tokens.length) {
	       	   val id_count_pair = tokens(i).split(":")
		   val id = id_count_pair(0).toInt
		   examples(index).doc.ids.add(id)
		   examples(index).doc.counts.add(id_count_pair(1).toDouble)
		   if (max_id < id) { max_id = id }
	       }	   

	       if (examples(index).doc.ids.size() > 0 || label_missing) {
	       	       index = index + 1
	       }
	   }
	   print("Num Features: "); println(max_id + 1)
	   print("Num Good Examples: "); println(index - 1)
	   var good_examples = new Array[PoissonExample](index - 1)
	   System.arraycopy(examples, 0, good_examples, 0, index - 1)
	   return good_examples
       }

       def split_examples(examples: Array[PoissonExample], slices: Int) : Array[Array[PoissonExample]] = {
       	   var retval = new Array[Array[PoissonExample]](slices)
	   val num_docs = examples.length / slices
	   var index = 0

	   for (s <- 0 until slices) {
	       retval(s) = new Array[PoissonExample](num_docs)
	       for (d <- 0 until num_docs) {
	       	   retval(s)(d) = examples(index)
		   index += 1
	       }
	   }
	   return retval
       }

       // This is stupid
        def get_num_examples(path: String):Int = {
	    val source = Source.fromFile(path)
            val lines = source.mkString.split("\n")
            source.close()
            return lines.length
	}
}

object PoissonConstants {
       val alpha = 0.0
       val chunk_size = 10000
}


object SimplePoissonRegression {
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: PoissonRegression <host> <input> <num_features> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "PoissonRegression")
        val path = args(1)
	val num_features = args(2).toInt
        val slices = if (args.length > 2) args(3).toInt else 1

	var indices = new Array[Int](PoissonConstants.chunk_size)

	var weights = new PoissonWeights(num_features)
	weights.random_init()
	var duals = new PoissonDuals(num_features)
	var grad = new PoissonGradient(num_features)

	val examples = PoissonDataReader.read(path, "full.txt")
	var converged = false
	var go = true
	var iter = 1

	val rand = new Random(new Date().getTime())
	val start = new Date().getTime()
	var time = start
        val runtime = 100

	while (go) {
	      LDAHelpers.generate_indices(indices, rand, examples.length)

	      PoissonRegression.gradient(examples, weights, indices, grad, PoissonConstants.alpha)
	      print("Objective: "); println(PoissonRegression.objective(examples, weights, indices, PoissonConstants.alpha))


	      //println("Weights: " + weights.toString)
	      //println("Grad: " + grad.toString)

	      // Update global parameters
	      var learning_rate = math.pow((iter), -1) / 1000
	      val change = PoissonRegression.update_weight(weights, duals, grad, learning_rate, iter)
	      print("Change: "); println(change)
	      grad.clear()
	      	      
	      var time = new Date().getTime()
	      go = !converged && ((time - start) / 1000 < runtime) && iter < 100000
	      print("Rajesh: "); println(go)
	      iter = iter + 1

	}
	// Write out the topics to file      	
	//TicTocLR.appendToFile("OnlineLDA.result", twd.toString)
        sc.stop()
    }
}


// Begin advancers
class PRChildToMaster (var grad: PoissonGradient, var iter:Int) extends Serializable {
    override def toString: String = {
        return "iter: " + iter.toString + "\n" + grad.toString
    }
}

// This is only deserialized once at the start of each new task
class PRState(@transient val num_examples: Int, @transient val alpha: Double, @transient val chunk_size: Int, var mutable_state: PRStateUpdate) extends Serializable{
      // The duals are meaningless on the children
      @transient val duals: PoissonDuals = new PoissonDuals(mutable_state.weights.num_features)
      override def clone() : PRState = {
      	       var retval = new PRState(num_examples, alpha, chunk_size, mutable_state.clone)
	       //System.arraycopy(duals, 0, retval.duals, 0, mutable_state.weights.num_features)
	       return retval
      }
}

class PRStateUpdate(var weights: PoissonWeights, var converged:Boolean, var iter:Int) extends Serializable 
{
	override def clone() : PRStateUpdate = {
		 return new PRStateUpdate(weights.clone, converged, iter); 
	}
} 
 
object PoissonAdvancer {
  type G = PRChildToMaster
  type P = PRState
   
  // To the Master 
   class MasterMessage (var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P] {
   	 override def toString: String = { return "" } 
   }

    // From the master
    class Diff (var id:Long, @transient message: G, @transient state: PRState) extends UpdatedProgressDiff[G,P] {
	  var update = state.mutable_state
    	  
	  def update(old_var : UpdatedProgress[G,P]) = {
              // The workers clone the state before using it, so this is "pretty" safe
              old_var.value.mutable_state.converged = update.converged
              old_var.value.mutable_state.iter = update.iter
              old_var.value.mutable_state.weights = update.weights 
    	  }
    	  
	  override def toString = "\n" + "id:" + id.toString + ":" + update.toString
     }

     object Modifier extends UpdatedProgressModifier[G,P] {
	val eps = 1e-4
	def makeMasterMessage (oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressMasterMessage[G,P] = {
       	    val dummy:P = null
       	    return new MasterMessage(oldVar.id, message, dummy)
        }
    
	def updateLocalDecideSend(oldVar: UpdatedProgress[G,P], message: G) : Boolean = {
            // always send G; no change to local state
            return true
    	}
    
	def zero(initialValue: PRState) = initialValue.clone

    	def masterAggregate (old_var: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {	    
	    old_var.value.mutable_state.iter += 1
	    var learning_rate = math.pow((old_var.value.mutable_state.iter), -1) / 1000 
	    val change = PoissonRegression.update_weight(old_var.value.mutable_state.weights, old_var.value.duals, message.grad, learning_rate, old_var.value.mutable_state.iter)

	    print("Change: "); println(change)
	             
	    if (change < eps && change > 1e-15) {
               old_var.value.mutable_state.converged = true
       	    } else {
               old_var.value.mutable_state.converged = false
            }
	    return new Diff(old_var.id, message, old_var.value)
     	}
     }
}


object AdvancersPoissonRegression {
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: AdvancersPoissonRegression <host> <input> <vocab_size> <num_topics> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "AdvancersPoissonRegression")
        val path = args(1)
	val num_features = args(2).toInt
        val slices = if (args.length > 2) args(3).toInt else 1
	
	// ARPAN: Load the full the documents, you may get better results with the partition
	val examples = PoissonDataReader.read(path, "full.txt")	

	var weights = new PoissonWeights(num_features)
	weights.random_init()	

	var mutable_state = new PRStateUpdate(weights, false, 0)
	var state = sc.updatedProgress(new PRState(examples.length, PoissonConstants.alpha, PoissonConstants.chunk_size, mutable_state), PoissonAdvancer.Modifier)
	
	    // TODO: This could be an RDD if we want to parallize easier
	    for (i <- sc.parallelize(0 until slices, slices)) {
	        var weights = new PoissonWeights(num_features)
	    
	        var indices = new Array[Int](PoissonConstants.chunk_size)
	        val rand = new Random(new Date().getTime())
		var grad = new PoissonGradient(state.value.mutable_state.weights.num_features)

	        // ARPAN: This line loads a partition of the data
	        // val example = PoissonDataReader.read(path, i.toString + ".txt")

	        var go = true
	        var iter = 1

	        val start = new Date().getTime()
	        var time = start
            	val runtime = 100

            	var period = new Date().getTime()

            	TicTocLR.appendToFile("ADVANCER_OLDA_32_RESULT", "Time  Per-word-perplexity")

	        while (go) {
	    	    var global_iter = state.value.mutable_state.iter
		    LDAHelpers.generate_indices(indices, rand, examples.length)
	      	    weights = state.value.mutable_state.weights.clone // RAJESH: CHECK THIS
		    PoissonRegression.gradient(examples, weights, indices, grad, PoissonConstants.alpha)
                    
		    val check = new Date().getTime()
                    if ((check - period) / 1000 >= 1.0 && i == 0) {
                       // ARPAN: Do something here?
                       period = new Date().getTime()
                    }
		    
		    
               	    if (iter % slices == 0) {
		       val objective = PoissonRegression.objective(examples, weights, indices, PoissonConstants.alpha)
                       print("Objective: "); println(objective)
		       //TicTocLR.appendToFile("DEBUG", "Objective at " + global_iter.toString + ": " + objective.toString)
		    }
                   
		   
		   state.advance(new PRChildToMaster(grad.clone, state.value.mutable_state.iter))
		   grad.clear()
		   var time = new Date().getTime()
		   go = ((time - start) / 1000 < runtime)
		   iter = iter + 1
	        }

	        // One last objective calculation
	        var global_iter = state.value.mutable_state.iter
	        LDAHelpers.generate_indices(indices, rand, examples.length)
	        weights = state.value.mutable_state.weights.clone
	        val objective = PoissonRegression.objective(examples, weights, indices, PoissonConstants.alpha)
		print("Objective: "); println(objective)
		//TicTocLR.appendToFile("DEBUG", "Objective at " + global_iter.toString + ": " + objective.toString)
        }

	
	// Write out the topics to file
	//TicTocLR.appendToFile("AdvancersOnlineLDA.result", state.value.mutable_state.twd.toString)
        sc.stop()
    }
}

// Define a gradient Accumulator
object PoissonGradAccumulatorParam extends AccumulatorParam[PoissonGradient] {
    def addInPlace(t1: PoissonGradient, t2: PoissonGradient): PoissonGradient = {
        var num_features = t1.grad.length
	var result = new PoissonGradient(num_features)
        for (i <- 0 until num_features) {
	    result.grad(i) = t1.grad(i) + t2.grad(i)
        }
        return result
    }

    def zero(initialValue: PoissonGradient): PoissonGradient = {
    	var num_features = initialValue.grad.length
        
	var grad = new PoissonGradient(num_features)
        for (i <- 0 until num_features) {
	    grad.grad(i) = 0
	}
        return grad
    }
}


object TicTocPoissonRegression {
    
    def main(args: Array[String]) {
    	 if (args.length < 2) {
            System.err.println("Usage: TicTocPoissonRegression <host> <input> <vocab_size> <num_topics> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "TicTocPoissonRegression")
        val path = args(1)
	val num_features = args(2).toInt
        val slices = if (args.length > 2) args(3).toInt else 1

	var indices = new Array[Int](PoissonConstants.chunk_size)
	
	// ARPAN: Load the full the documents, you may get better results with the partition
	val all_examples = PoissonDataReader.read(path, "full.txt")	

	var weights = new PoissonWeights(num_features)
	weights.random_init()        
	
    	val keyed_examples = all_examples.zipWithIndex.map { 
            case (d, i) => (i % slices, d)
    	}
    
	// turn that indo RDD
    	var rdd = sc.parallelize(keyed_examples).groupByKey().cache
	var duals = new PoissonDuals(num_features)

	var go = true
	var iter = 1
	val start = new Date().getTime()
	var time = start
        val runtime = 100
	val eps = 1e-4

        var period = new Date().getTime()

        TicTocLR.appendToFile("TT_OLDA_32_RESULT", "Time  Per-word-perplexity") 

	var grad_acc = sc.accumulator(new PoissonGradient(num_features))(PoissonGradAccumulatorParam)
	while (go) {

	        rdd.foreach { (dataset) =>
	            var (index, examples) = dataset
	            var indices = new Array[Int](PoissonConstants.chunk_size)
	            val rand = new Random(new Date().getTime())	
		    var grad = new PoissonGradient(num_features)
	       
	            LDAHelpers.generate_indices(indices, rand, examples.length)
		    PoissonRegression.no_scale_gradient(examples.toArray, weights, indices, grad)
	            grad_acc += grad
	        }
	
	         val rand = new Random(new Date().getTime())	
	        // ARPAN: write out the bound with a time stamp every x seconds (i.e. not every time)
	        LDAHelpers.generate_indices(indices, rand, all_examples.length)
		val objective = PoissonRegression.objective(all_examples, weights, indices, PoissonConstants.alpha)
		print("Objective: "); println(objective)
		TicTocLR.appendToFile("DEBUG", "Objective at " + iter.toString + ": " + objective.toString)

	        // Update the weights
	        //TicTocLR.appendToFile("DEBUG", "TWD: " + twd.dist(0)(0).toString)
		PoissonRegression.scale_gradient(grad_acc.value, all_examples.length, slices * PoissonConstants.chunk_size, weights, PoissonConstants.alpha)  
		val learning_rate = math.pow((iter), -1) / 1000
	        val change = PoissonRegression.update_weight(weights, duals, grad_acc.value, learning_rate, iter)

	        grad_acc.value.clear()

	        // Figure out to stop or not
	        var time = new Date().getTime()
	        val converged = change < eps
	        go = ((time - start) / 1000 < runtime)
	        iter = iter + 1
	    }

	
	    // Write out the topics to file
	    //TicTocLR.appendToFile("TicTocOnlineLDA.result", state.value.mutable_state.twd.toString)
        sc.stop()
     }
}
