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
class Document extends Serializable {
      var counts = new ArrayList[Int]();
      var ids = new ArrayList[Int]();
      var num_words = 0
}

class TopicWordDistribution(val num_topics: Int, val num_words: Int) extends Serializable {
      var dist = new Array[Array[Double]](num_topics);
      for (i <- 0 until num_topics) {
      	  dist(i) = new Array[Double](num_words)
      }

      // Initatializes each row to a random draw from a dirichlet with parameter eta
      // TODO: Currently assumes the parameter vector has all the same entries
      def random_init(eta: Double) {
	  var rng = new RandomDataImpl()
	  rng.reSeed()
	  for (t <- 0 until num_topics) {
	      var sum = 0.0
	      for (i <- 0 until num_words) {
	      	  dist(t)(i) = rng.nextGamma(eta, 1)
		  sum = sum + dist(t)(i)
	      }

	      // Normalize
	      for (i <- 0 until num_words) {
	      	  dist(t)(i) = dist(t)(i) / sum
	      }
	      
	   }
      }

      def unif_init() {
      	  for (t <- 0 until num_topics) {
	      for (i <- 0 until num_words) {
	      	  dist(t)(i) = 1.0 / num_words
	      }
	   }
      }

      def file_init(file: String) {
      	  var topic = 0
      	  for (line <- scala.io.Source.fromFile(new File(file)).getLines()) {
	       if (line.length() > 0) {
	       	  val tokens = line.split(",")
		  var j = 0
	       	  for (lambda_p <- tokens) {
		      if (lambda_p.length() > 0) {
		      	 dist(topic)(j) = lambda_p.toDouble
			 j = j + 1
		      }
	       	  }
		  if (j != num_words) {
		      throw new IOException("Num words in file doesn't match command line args")
		  }
		  topic = topic + 1
	       }
	   }
	   if (topic != num_topics) {
	      throw new IOException("Num topics in file doesn't match command lin args")
	   }
      }

      def twd_init(other: TopicWordDistribution) {
      	  for (t <- 0 until num_topics) {
	      for (w <- 0 until num_words) {
	      	  dist(t)(w) = other.dist(t)(w)
	      }
	  }
      }

      override def toString() : String  = {
	  var result = ""
	  for (t <- 0 until num_topics) {
	      var line = ""
	      for (w <- 0 until num_words) {
	      	  line = line + dist(t)(w) + ","
	      }
	      result = result + line.substring(0, line.length - 1) + "\n"
	  }
	  result = result + "\n"
	  return result
      }

      override def clone() : TopicWordDistribution = {  
  	  var retval = new TopicWordDistribution(num_topics, num_words);
	  for (i <- 0 until num_topics) {
	      retval.dist(i) = dist(i).clone
	  }
	  return retval
      }
}

class TopicESStats(val num_topics: Int, val num_words: Int) extends Serializable {
      var stats = new Array[Array[Double]](num_topics)
      for (i <- 0 until num_topics) {
      	  stats(i) = new Array[Double](num_words)
      }

      def clear() {
      	  for (i <- 0 until num_topics) {
	      for (j <- 0 until num_words) {
	      	  stats(i)(j) = 0
	      }
	  }
      }

      override def clone() : TopicESStats = {  
  	  var retval = new TopicESStats(num_topics, num_words)
	  for (i <- 0 until num_topics) {
	      retval.stats(i) = stats(i).clone
	  }
	  return retval
      }
}

class ExpELogBeta(val num_topics: Int, val num_words: Int)  extends Serializable {
      var value = new Array[Array[Double]](num_topics)
      for (i <- 0 until num_topics) {
      	  value(i) = new Array[Double](num_words)
      }

      override def clone() : ExpELogBeta = {
      	  var retval = new ExpELogBeta(num_topics, num_words)
	  for (i <- 0 until num_topics) {
	      retval.value(i) = value(i).clone
	  }
	  return retval
      }
}

object LDAHelpers {

       def string_2d_array(x: Array[Array[Double]]) : String = {
       	   var ret = ""
	   for (row <- x) {
	       var line = ""
	       for (col <- row) {
	       	   line = line + col.toString + ","
	       }
	       ret = ret + "\n" +  line
	   }
	   return ret
       }
       
       def update_topic_word_dist(twd: TopicWordDistribution, sstats: TopicESStats, learning_rate: Double,
       	   			  num_docs: Int, batch_size: Int, eta: Double) : Double = {

	   var change = 0.0
	   for (t <- 0 until twd.num_topics) {
	       for (w <- 0 until twd.num_words) {
	       	   val update = (eta + num_docs * sstats.stats(t)(w) / batch_size) 
	       	   change += math.abs(update - twd.dist(t)(w))
		   twd.dist(t)(w) = (1 - learning_rate) * twd.dist(t)(w) + learning_rate * update

	       }
	   }
	   return change * learning_rate / twd.num_topics / twd.num_words
       }

       def update_e_log_beta(twd: TopicWordDistribution, e_log_beta: ExpELogBeta) {
	   for (t <- 0 until twd.num_topics) {
	       e_log_beta.value(t) = elem_exp(dirichlet_expectation(twd.dist(t)))
	   }
       }

       def inner_product(vec1 : ArrayList[Int], vec2: Array[Double]) : Double = {
       	   if (vec1.size() != vec2.length) {
	      throw new IllegalArgumentException("Vectors to inner product need to have the same length")
	   }
	   var retval = 0.0
	   for (i <- 0 until vec1.size()) {
	       retval = retval + vec1.get(i) * vec2(i)
	   }
	   return retval
       }

       def vec_gammaln(vec: Array[Double]) : Array[Double] = {
       	   var retval = new Array[Double](vec.length)
	   for (i <- 0 until vec.length) {
	       retval(i) = logGamma(vec(i))
	   }
	   return retval
       }

       def perplexity_from_bound(documents: Array[Document], indices: Array[Int], bound: Double) : Double = {
       	   var num_words = 0
	   for (ind <- indices) {
	       num_words += documents(ind).num_words
	   }
	   var per_word_bound = bound * indices.length / documents.length / num_words
	   return math.exp(-per_word_bound)
       }       

       // Approximates the global objective based on a subset of the documents
       def approx_bound(documents: Array[Document], twd: TopicWordDistribution, expElogbeta: ExpELogBeta,  
       	   indices: Array[Int], gammas: Array[Array[Double]], alpha: Double, eta: Double) : Double = {
	   var score = 0.0
	   for (i <- 0 until indices.length) {
	       val index = indices(i)
	       val doc = documents(index)
	       val gammad = gammas(i)
	       var Elogtheta = dirichlet_expectation(gammad)
	       var expElogthetad = elem_exp(Elogtheta)

	       var lphi_norm = log_phi_normalization(Elogtheta, expElogbeta, doc.ids)
	       score += inner_product(doc.counts, lphi_norm)

	       // E[log p(theta|alpha) - log q(theta | gamma)]
	       score -= sum(vec_element_wise_product(increment(-alpha, gammad.clone), Elogtheta))
	       score += -gammad.length * logGamma(alpha) + sum(vec_gammaln(gammad))
	       score += logGamma(alpha * twd.num_topics) - logGamma(sum(gammad))
	   }

	   score = (score * documents.length) / indices.length


	   // E[log p(beta|eta) - log q(beta|lambda)]
	   for (t <- 0 until twd.num_topics) {
	       var topic_clone = twd.dist(t).clone
	       var Elogbetat = elem_log(expElogbeta.value(t).clone)
	       score -= sum(vec_element_wise_product(increment(-eta, topic_clone), Elogbetat))
	       	       if (score.isNaN) { println("RAJESH: 6" ) }	   
	       score += sum(increment(-logGamma(eta), vec_gammaln(twd.dist(t))))
	       score += logGamma(eta * twd.num_words) - logGamma(sum(twd.dist(t)))
	   }
	   return score
       }

       def elem_log(x : Array[Double]) : Array[Double] = {
       	   for (i <- 0 until x.length) {
	       x(i) = math.log(x(i))
	   }   
	   return x
       }
       
       
       def sstats(documents: Array[Document], twd: TopicWordDistribution, expElogbeta: ExpELogBeta,  
       	   indices: Array[Int], num_examples: Int, sstats: TopicESStats, alpha: Double, gammas: Array[Array[Double]]) {
       	   
	   var num_processed = 0
	   val change_thresh = 1e-4
	   val num_topics = twd.num_topics

	   for (index <- indices) {
	       val doc = documents(index)
	       val ids = doc.ids
	       val counts = doc.counts	       

	       var gammad = gammarnd(100, 100, num_topics)
	       var Elogtheta = dirichlet_expectation(gammad)
	       var expElogthetad = elem_exp(Elogtheta) // Num Topics by 

	       var phi_norm = phi_normalization(expElogthetad, expElogbeta, ids)
	       var exit = false
	       var iter = 0

	       // Technically this should be until convergence, but good progress can be made in a fixed number of iterations
	       while (iter < 100 && !exit) {
	       	   // RAJESH: This needs to be a deep copy
	       	   var lastgamma = gammad
  
		   // RAJESH: DEFINE ALPHA!!!
		   gammad = increment(alpha, vec_element_wise_product(expElogthetad, gamma_count_update(doc, phi_norm, expElogbeta)))
		   Elogtheta = dirichlet_expectation(gammad)
		   expElogthetad = elem_exp(Elogtheta)
		   phi_norm = phi_normalization(expElogthetad, expElogbeta, ids)

		   var change = mean_absolute_difference(gammad, lastgamma)
		   if (change < change_thresh) {
		      exit = true
		   }
		   iter = iter + 1
	       }
	       if (gammas != null) {
	       	  // There's a new gammad created each loop
	       	  gammas(num_processed) = gammad
	       	  num_processed += 1
	       } 
	       update_sstats(sstats, expElogthetad, doc, phi_norm)
	   }

	  final_sstats(sstats, expElogbeta)
       }

       def update_sstats(sstats: TopicESStats, expElogthetad: Array[Double], doc: Document, phi_norm: Array[Double]) {
	   for (t <- 0 until sstats.num_topics) {
	       for (i <- 0 until  doc.counts.size()) {
		   val index = doc.ids.get(i)
		   sstats.stats(t)(index) = sstats.stats(t)(index) + expElogthetad(t) * doc.counts.get(i) / phi_norm(i)  
	       }
	   }
       }

       def final_sstats(sstats: TopicESStats, e_log_beta: ExpELogBeta) {
       	   for (t <- 0 until sstats.num_topics) {
	       for (w <- 0 until sstats.num_words) {
	       	   sstats.stats(t)(w) = sstats.stats(t)(w) * e_log_beta.value(t)(w)
	       }
	   }
       } 

       // TODO: We could pass gamma in, compute the change on the fly, and make this more efficient       
       def mean_absolute_difference(old: Array[Double], new_val: Array[Double]) : Double = {
       	   var diff = 0.0
	   for (i <- 0 until old.length) {
	       diff = diff + math.abs(old(i) - new_val(i))
	   }
	   return diff / old.length
       }
       
       // Computes out(i) = a(i) * out(i), returns out as a convenience
       def vec_element_wise_product(a: Array[Double], out: Array[Double]) : Array[Double] =  {
       	   for (i <- 0 until out.length) {
	       out(i) = a(i) * out(i)
	   }
	   return out
       }

       def gamma_count_update(doc: Document, phi_norm: Array[Double], e_log_beta: ExpELogBeta): Array[Double] = {
       	   var retval = new Array[Double](e_log_beta.num_topics)
	   for (t <- 0 until e_log_beta.num_topics) {
	       for (i <- 0 until doc.ids.size()) {
	       	   val index = doc.ids.get(i)
		   retval(t) = retval(t) + doc.counts.get(i) / phi_norm(i) * e_log_beta.value(t)(index)
	       }
	   }
	   return retval
       }

       // Increments an each element of x in-place, returns x as convenience
       def increment(increment: Double, x: Array[Double]) : Array[Double] = {
	   for (i <- 0 until x.length) {
	       x(i) = x(i) + increment
	   }
	   return x
       }

       // END TODO

       // Used for expectation of log(theta) and expectation of log(beta)
       def dirichlet_expectation(alpha: Array[Double]) : Array[Double] = {
       	   var retval = new Array[Double](alpha.length)
	   val normalization = digamma(sum(alpha));
	   for (k <- 0 until alpha.length) {
	       retval(k) = digamma(alpha(k)) - normalization
	   }
	   return retval
       }

       // normalization from the implicit phi
       def phi_normalization(expElogthetad: Array[Double], expElogbeta: ExpELogBeta, ids: ArrayList[Int]) : Array[Double] = {
       	   var retval = new Array[Double](ids.size())
	   for (i <- 0 until ids.size()) {
	       val index = ids.get(i)
	       var inner_product = 0.0
	       for (t <- 0 until expElogbeta.num_topics) {
	       	   inner_product = inner_product + expElogbeta.value(t)(index) * expElogthetad(t)
	       }
	       retval(i) = inner_product + 1e-70
	   }
	   	   
	   return retval
       }

       def log_phi_normalization(Elogthetad: Array[Double], expElogbeta: ExpELogBeta, ids: ArrayList[Int]) : Array[Double] = {
       	   var retval = new Array[Double](ids.size())
	   
	   for (i <- 0 until ids.size()) {
	       val index = ids.get(i)
	       var max = scala.Double.NegativeInfinity
	       var temp = new Array[Double](expElogbeta.num_topics)

	       for (t <- 0 until expElogbeta.num_topics) {
		   temp(t) = math.log(expElogbeta.value(t)(index))
		   temp(t) = temp(t) + Elogthetad(t)
		   if (temp(t) > max) { max = temp(t) }
	       }
	       retval(i) = math.log(sum(elem_exp(increment(-max, temp)))) + max 
	   }
	   return retval
       }

       // Elementwise exponentiation
       def elem_exp(x: Array[Double]) : Array[Double] = {
       	   var retval = new Array[Double](x.length)
	   for (i <- 0 until x.length) {
	       retval(i) = math.exp(x(i))
	   }
	   return retval
       }

       // Computes the sum of x
       def sum(x: Array[Double]) : Double = {
	   var retval = 0.0
	   for (i <- 0 until x.length) {
	       retval = retval + x(i)
	   }
	   return retval
       }

       def gammarnd(alpha: Double, beta: Double, size: Int) : Array[Double] = {
       	   var rng = new RandomDataImpl()
	   rng.reSeed()
	   var retval = new Array[Double](size)
	   for (i <- 0 until size) {
	       retval(i) = rng.nextGamma(alpha, beta)
	   }
	   return retval
       }

       def generate_indices(ind: Array[Int], r:Random, max:Int) {
       	   for (i <- 0 until ind.length) {
	       ind(i) = r.nextInt(max)
	   }
       }
}

object OnlineLDADataReader {
       def read(path: String, id: String) : Array[Document] = {
       	   val doc_path : String = path + "/" + id
	   var documents = new Array[Document](get_num_documents(doc_path))
	   var index = 0
	   for (line <- scala.io.Source.fromFile(new File(doc_path)).getLines()) {
	       documents(index) = new Document()
	       val tokens = line.split(" ");
	       for (i <- 1 until tokens.length) {
	       	   val id_count_pair = tokens(i).split(":")
		   documents(index).ids.add(id_count_pair(0).toInt)
		   documents(index).counts.add(id_count_pair(1).toInt)
		   documents(index).num_words += id_count_pair(1).toInt
	       }	   
	       index = index + 1
	   }
	   return documents
       }

       def split_documents(documents: Array[Document], slices: Int) : Array[Array[Document]] = {
       	   var retval = new Array[Array[Document]](slices)
	   val num_docs = documents.length / slices
	   var index = 0

	   for (s <- 0 until slices) {
	       retval(s) = new Array[Document](num_docs)
	       for (d <- 0 until num_docs) {
	       	   retval(s)(d) = documents(index)
		   index += 1
	       }
	   }
	   return retval
       }

       // This is stupid
        def get_num_documents(path: String):Int = {
	    val source = Source.fromFile(path)
            val lines = source.mkString.split("\n")
            source.close()
            return lines.length
	}
}


object OnlineLDA {
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineLDA <host> <input> <vocab_size> <num_topics> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "OnlineLDA")
        val path = args(1)
	val num_words = args(2).toInt
	val num_topics = args(3).toInt
        val slices = if (args.length > 2) args(4).toInt else 1

        val chunk_size = 32
	val alpha = 1.0 / num_topics
	val eta = 1.0 / num_topics
	var indices = new Array[Int](chunk_size)    	

	var twd = new TopicWordDistribution(num_topics, num_words)
	twd.random_init(eta)
	var exp_e_log_beta = new ExpELogBeta(num_topics, num_words)
	LDAHelpers.update_e_log_beta(twd, exp_e_log_beta);
	var sstats = new TopicESStats(num_topics, num_words) 
	var gamma = new Array[Array[Double]](chunk_size)	

	val documents = OnlineLDADataReader.read(path, "full.txt")
	var converged = false
	var go = true
	var iter = 1

	val rand = new Random(new Date().getTime())	
	val start = new Date().getTime()
	var time = start
        val runtime = 32

	while (go) {
	      LDAHelpers.generate_indices(indices, rand, documents.length)
	      LDAHelpers.sstats(documents, twd, exp_e_log_beta, indices, chunk_size, sstats, alpha, gamma)

	      if (iter % 1 == 0) {
	      	 print("Approx bound at " + iter.toString + ": ")
		 val bound = LDAHelpers.approx_bound(documents, twd, exp_e_log_beta, indices, gamma, alpha, eta)
		 print(bound)
		 print(" per word perplexity: ")
		 println(LDAHelpers.perplexity_from_bound(documents, indices, bound))
	      }	      
	      	      
	      // Update global parameters
              var learning_rate = math.pow((iter + 1024), -.55)
	      print("Change: "); println(LDAHelpers.update_topic_word_dist(twd, sstats, learning_rate, documents.length, chunk_size, eta));
	      sstats.clear()
	      LDAHelpers.update_e_log_beta(twd, exp_e_log_beta);
	      
	      var time = new Date().getTime()
	      go = !converged && ((time - start) / 1000 < runtime)
	      print("Rajesh: "); println(go)
	      iter = iter + 1
	}
	// Write out the topics to file      	
	//TicTocLR.appendToFile("OnlineLDA.result", twd.toString)
        sc.stop()
    }
}


// Begin advancers
class LDAChildToMaster (var sstats: TopicESStats, var iter:Int) extends Serializable {
    override def toString: String = {
        return "iter: " + iter.toString + "\n" + sstats.toString
    }
}

// This is only deserialized once at the start of each new task
class OnlineLDAState(@transient val num_docs: Int, @transient val eta: Double, @transient val chunk_size: Int, var mutable_state: OnlineLDAStateUpdate) extends Serializable{

     // Why is this needed?
     override def clone() : OnlineLDAState = {
	 return new OnlineLDAState(num_docs, eta, chunk_size, mutable_state.clone)
     }
}

class OnlineLDAStateUpdate(var twd: TopicWordDistribution, var converged:Boolean, var iter:Int) extends Serializable 
{
     override def clone() : OnlineLDAStateUpdate = {
         return new OnlineLDAStateUpdate(twd.clone, converged, iter)
     }
} 
 
object OnlineLDAAdvancer {
  type G = LDAChildToMaster
  type P = OnlineLDAState
   
  // To the Master 
   class MasterMessage (var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P] {
   	 override def toString: String = { return "" } 
   }

    // From the master
    class Diff (var id:Long, @transient message: G, @transient state: OnlineLDAState) extends UpdatedProgressDiff[G,P] {
	  var update = state.mutable_state
    	  
	  def update(old_var : UpdatedProgress[G,P]) = {
              // The workers clone the state before using it, so this is "pretty" safe
              old_var.value.mutable_state.converged = update.converged
              old_var.value.mutable_state.iter = update.iter
              old_var.value.mutable_state.twd = update.twd 
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
    
	def zero(initialValue: OnlineLDAState) = initialValue.clone

    	def masterAggregate (old_var: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {	    

	    old_var.value.mutable_state.iter += 1
	    var learning_rate = math.pow((old_var.value.mutable_state.iter + 1024), -.55) 
	    var change = LDAHelpers.update_topic_word_dist(old_var.value.mutable_state.twd, message.sstats, learning_rate, old_var.value.num_docs, old_var.value.chunk_size, old_var.value.eta)

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


object AdvancersOnlineLDA {
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineLDA <host> <input> <vocab_size> <num_topics> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "AdvancersOnlineLDA")
        val path = args(1)
	val num_words = args(2).toInt
	val num_topics = args(3).toInt
        val slices = if (args.length > 2) args(4).toInt else 1

        // ARPAN: You can play around with the chunk_size. A larger chunk_size reduces the amount of communication
	val chunk_size = 32
	val alpha = 1.0 / num_topics
	val eta = 1.0 / num_topics

	var num_docs = 10000
	var twd = new TopicWordDistribution(num_topics, num_words)
	twd.random_init(eta)
	var update = new OnlineLDAStateUpdate(twd, false, 0)
	var state = sc.updatedProgress(new OnlineLDAState(num_docs, eta, chunk_size, update), OnlineLDAAdvancer.Modifier)

	
	// ARPAN: Load the full the documents, you may get better results with the partition
	val documents = OnlineLDADataReader.read(path, "full.txt")
	
	// TODO: This could be an RDD if we want to parallize easier
	for (i <- sc.parallelize(0 until slices, slices)) {
	    // This is a hack to get around the specific deserializer implementation that OnlineLDAState needs
	    var exp_e_log_beta = new ExpELogBeta(num_topics, num_words)
	    var twd = new TopicWordDistribution(num_topics, num_words)
	    
	    var indices = new Array[Int](chunk_size)
	    var sstats = new TopicESStats(num_topics, num_words) 
	    var gamma = new Array[Array[Double]](chunk_size)
	    val rand = new Random(new Date().getTime())	

	    // ARPAN: This line loads a partition of the data
	    // val documents = OnlineLDADataReader.read(path, i.toString + ".txt")

	    var go = true
	    var iter = 1

	    val start = new Date().getTime()
	    var time = start
            val runtime = 100

	    while (go) {
	    	  var global_iter = state.value.mutable_state.iter
		  LDAHelpers.generate_indices(indices, rand, documents.length)
	      	  twd.twd_init(state.value.mutable_state.twd.clone)
		  LDAHelpers.update_e_log_beta(twd, exp_e_log_beta)
		  LDAHelpers.sstats(documents, twd, exp_e_log_beta, indices, chunk_size, sstats, alpha, gamma)

		  //var max_sstat = scala.Double.NegativeInfinity
		  //for (t <- 0 until num_topics) {
		  //    for (w <- 0 until num_words) {
		  //    	  if (max_sstat < math.abs(sstats.stats(t)(w))) { max_sstat = math.abs(sstats.stats(t)(w)) }
		  //    }
		  //}

		  // ARPAN: write out the bound with a time stamp every x seconds
		  // Is there a faster way to write files?
		  if (iter % 1 == 0 && i == 0) {
                    val bound = LDAHelpers.approx_bound(documents, twd, exp_e_log_beta, indices, gamma, alpha, eta)
                    TicTocLR.appendToFile("DEBUG", "Approx bound at " + global_iter.toString + ": " + bound.toString)
                    TicTocLR.appendToFile("DEBUG", "Per word perplexity: " + LDAHelpers.perplexity_from_bound(documents, indices, bound).toString)
		  }

		  
		  //if (max_sstat < 10000) {
		     state.advance(new LDAChildToMaster(sstats.clone, state.value.mutable_state.iter))
		  //} else {
		  //  TicTocLR.appendToFile("DEBUG", "OK")
		 // }

		  sstats.clear()
		  var time = new Date().getTime()
		  go = !state.value.mutable_state.converged && ((time - start) / 1000 < runtime) && iter < 1000
		  iter = iter + 1
	    }

	    // One last bound calculation
	    var global_iter = state.value.mutable_state.iter
	    LDAHelpers.generate_indices(indices, rand, documents.length)
	    twd.twd_init(state.value.mutable_state.twd.clone)
	    LDAHelpers.update_e_log_beta(twd, exp_e_log_beta)
	    LDAHelpers.sstats(documents, twd, exp_e_log_beta, indices, chunk_size, sstats, alpha, gamma)
	    val bound = LDAHelpers.approx_bound(documents, twd, exp_e_log_beta, indices, gamma, alpha, eta)
            TicTocLR.appendToFile("DEBUG", "Approx bound at " + global_iter + ": " + bound.toString)
            TicTocLR.appendToFile("DEBUG", "Per word perplexity: " + LDAHelpers.perplexity_from_bound(documents, indices, bound).toString)
	}

	
	// Write out the topics to file
	//TicTocLR.appendToFile("AdvancersOnlineLDA.result", state.value.mutable_state.twd.toString)
        sc.stop()
     }
}

// Define a sstats Accumulator
object SStatsAccumulatorParam extends AccumulatorParam[TopicESStats] {
    def addInPlace(t1: TopicESStats, t2: TopicESStats): TopicESStats = {
        var result = new TopicESStats(t1.num_topics, t1.num_words)
        for (i <- 0 until t1.num_topics){
	    for (j <- 0 until t1.num_words) {
	    	result.stats(i)(j) = t1.stats(i)(j) + t2.stats(i)(j)
	    }
        }
        return result
    }

    def zero(initialValue: TopicESStats): TopicESStats = {
        var stats = new TopicESStats(initialValue.num_topics, initialValue.num_words)
        for (i <- 0 until initialValue.num_topics) {
	    for (j <- 0 until initialValue.num_words) {
            	stats.stats(i)(j) = 0
	    }
        }
        return stats
    }
}


object TicTocOnlineLDA {
    
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineLDA <host> <input> <vocab_size> <num_topics> <slices>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "TicTocOnlineLDA")
        val path = args(1)
	val num_words = args(2).toInt
	val num_topics = args(3).toInt
        val slices = if (args.length > 2) args(4).toInt else 1

	// ARPAN: You can play around this with a bit too
        val chunk_size = 32
	val alpha = 1.0 / num_topics
	val eta = 1.0 / num_topics
	val eps = 1e-4

	var num_docs = 10000
	var twd = new TopicWordDistribution(num_topics, num_words)
	twd.random_init(eta)

	val all_documents = OnlineLDADataReader.read(path, "full.txt")
    	val keyed_documents = all_documents.zipWithIndex.map { 
            case (d, i) => (i % slices, d)
    	}
    
	// turn that indo RDD
    	var rdd = sc.parallelize(keyed_documents).groupByKey().cache
	var exp_e_log_beta = new ExpELogBeta(num_topics, num_words)

	// For the one bound calculation
	var indices = new Array[Int](chunk_size)
	val rand = new Random(new Date().getTime())	
	var sstats = new TopicESStats(num_topics, num_words) 
	var gamma = new Array[Array[Double]](chunk_size)	

	var go = true
	var iter = 1
	val start = new Date().getTime()
	var time = start
        val runtime = 1000

	while (go) {
	    // This is a hack to get around the specific deserializer implementation that OnlineLDAState needs
	    LDAHelpers.update_e_log_beta(twd, exp_e_log_beta)	    
	    var sstats_acc = sc.accumulator(new TopicESStats(num_topics, num_words))(SStatsAccumulatorParam)

	    rdd.foreach { (dataset) =>
	       var (index, documents) = dataset
	       var indices = new Array[Int](chunk_size)
	       var sstats = new TopicESStats(num_topics, num_words) 
	       var gamma = new Array[Array[Double]](chunk_size)	
	       val rand = new Random(new Date().getTime())	
	       
	       LDAHelpers.generate_indices(indices, rand, documents.length)
	       LDAHelpers.sstats(documents.toArray, twd, exp_e_log_beta, indices, chunk_size, sstats, alpha, gamma)
	       sstats_acc += sstats
	    }
	
	    // ARPAN: write out the bound with a time stamp every x seconds (i.e. not every time)
	    LDAHelpers.generate_indices(indices, rand, all_documents.length)
	    LDAHelpers.sstats(all_documents, twd, exp_e_log_beta, indices, chunk_size, sstats, alpha, gamma)
	    val bound = LDAHelpers.approx_bound(all_documents, twd, exp_e_log_beta, indices, gamma, alpha, eta)
            TicTocLR.appendToFile("DEBUG", "Approx bound at " + iter + ": " + bound.toString)
            TicTocLR.appendToFile("DEBUG", "Per word perplexity: " + LDAHelpers.perplexity_from_bound(all_documents, indices, bound).toString)

	    // Update Lambda
	    TicTocLR.appendToFile("DEBUG", "TWD: " + twd.dist(0)(0).toString) 
	    var learning_rate = math.pow((iter + 1024), -.55)
	    val change = LDAHelpers.update_topic_word_dist(twd, sstats_acc.value, learning_rate, all_documents.length, slices * chunk_size, eta)

	    sstats.clear()
	    sstats_acc.value.clear()

	    // Figure out to stop or not
	    var time = new Date().getTime()
	    val converged = change < eps
	    go = !converged && ((time - start) / 1000 < runtime) && iter < 1000
	    iter = iter + 1
	}

	
	// Write out the topics to file
	//TicTocLR.appendToFile("TicTocOnlineLDA.result", state.value.mutable_state.twd.toString)
        sc.stop()
     }
}
