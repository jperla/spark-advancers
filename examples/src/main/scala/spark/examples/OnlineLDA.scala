package spark

import spark._
import scala.io.Source
import scala.collection.mutable._
import scala.util.Random
import java.util.Date
import java.util.ArrayList
import org.apache.commons.math.special.Gamma.digamma
import org.apache.commons.math._
import org.apache.commons.math.random._
import java.io.File

class Document {
      var counts = new ArrayList[Int]();
      var ids = new ArrayList[Int]();
}

class TopicWordDistribution(val num_topics: Int, val num_words: Int) {
      var dist = new Array[Array[Double]](num_topics);
      for (i <- 0 until num_topics) {
      	  dist(i) = new Array[Double](num_words)
      }

      // Initatializes each row to a random draw from a dirichlet with parameter eta
      // TODO: Currently assumes the parameter vector has all the same entries
      def random_init(eta: Double) {
	  var rng = new RandomDataImpl()
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
}

class TopicESStats(val num_topics: Int, val num_words: Int) {
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
}

class ExpELogBeta(val num_topics: Int, val num_words: Int) {
      var value = new Array[Array[Double]](num_topics)
      for (i <- 0 until num_topics) {
      	  value(i) = new Array[Double](num_words)
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
       
       // TODO: set eta
       def update_topic_word_dist(twd: TopicWordDistribution, sstats: TopicESStats, num_iter: Int,
       	   			  num_docs: Int, batch_size: Int, eta: Double) {
       	   var rho_t = 1.0 / num_iter
	   println("Start sttats")
	   for (t <- 0 until twd.num_topics) {
	       println(sstats.stats(t)(1))
	       for (w <- 0 until twd.num_words) {
	       	   val update = (eta + num_docs * sstats.stats(t)(w) / batch_size) 
	       	   twd.dist(t)(w) = (1 - rho_t) * twd.dist(t)(w) + rho_t * update

	       }
	   }

	   println("lambda: "); println(string_2d_array(twd.dist))
       }

       def update_e_log_beta(twd: TopicWordDistribution, e_log_beta: ExpELogBeta) {       	   
	   for (t <- 0 until twd.num_topics) {
	       e_log_beta.value(t) = elem_exp(dirichlet_expectation(twd.dist(t)))
	   }
	   print("beta: "); println(e_log_beta.value(0)(0))
       }

       def sstats(documents: Array[Document], twd: TopicWordDistribution, expElogbeta: ExpELogBeta,  
       	   start_index: Int, num_examples: Int, sstats: TopicESStats, alpha: Double) {
       	   
	   val change_thresh = 1e-4
	   val num_topics = twd.num_topics

	   for (i <- 0 until num_examples) {
	       val index = (i + start_index) % documents.length
	       val doc = documents(index)
	       val ids = doc.ids
	       val counts = doc.counts
	       
	       var gammad = gammarnd(100, 1.0 / 100, num_topics)
	       var Elogtheta = dirichlet_expectation(gammad)
	       var expElogthetad = elem_exp(Elogtheta) // Num Topics by 
	     
	       var phi_norm = phi_normalization(expElogthetad, expElogbeta, ids)
	       var exit = false
	       var iter = 0

	       println("Beta")
	       for (row <- expElogbeta.value) {
	       	   var line = ""
		   for (col <- row) {
		       line = line + col.toString + ","
		   }
		   println(line)
	       }

	       // Technically this should be until convergence, but good progress can be made in a fixed number of iterations
	       while (iter < 100 && !exit) {
	       	   // RAJESH: This needs to be a deep copy
	       	   var lastgamma = gammad

		   print("gammad " + iter.toString + " : ")
	       	   for (g <- gammad) {
	       	       print(g); print(", ")
	       	   }
	       	   println("")
		   print("phi_norm: "); 
		   for (p <- phi_norm) {
		       print(p); print(", ")
		   }
		   println("")
		   
		   print("expElogthetad: "); 
		   for (g <- expElogthetad) {
		           print(g); print(", ")
		   }
		   println("")
  
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

	       print("gammad: ")
	       for (g <- gammad) {
	       	   print(g); print(", ")
	       }
	       println("")

	        println("")
		   print("phi_norm: "); 
		   for (p <- phi_norm) {
		       print(p); print(", ")
		   }
		   println("")
		   
		   print("expElogthetad: "); 
		   for (g <- expElogthetad) {
		           print(g); print(", ")
		   }
		   println("")
	       
	       update_sstats(sstats, expElogthetad, doc, phi_norm)
	       println("Update sstats")
	       for (row <- sstats.stats) {
	       	   var line = ""
		   for (col <- row) {
		       line = line + col.toString + ","
		   }
		   println(line)
	       }
	   }

	  final_sstats(sstats, expElogbeta)
	  println("Final sstats")
	       for (row <- sstats.stats) {
	       	   var line = ""
		   for (col <- row) {
		       line = line + col.toString + ","
		   }
		   println(line)
	       }
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

       // RAJESH TODO: We could pass gamma in, compute the change on the fly, and make this more efficient       
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
	   print("Gamma count update: ")
	   for (g <- retval) {
	       print(g); print(", ")
	   }
	   println("")
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
	   var retval = new Array[Double](size)
	   for (i <- 0 until size) {
	       retval(i) = rng.nextGamma(alpha, beta)
	   }
	   return retval
       }
}

object OnlineLDADataReader {
       def read(path: String, id: Int) : Array[Document] = {
       	   val doc_path : String = path + "/" + id.toString +  ".txt"
	   var documents = new Array[Document](get_num_documents(doc_path))
	   var index = 0
	   for (line <- scala.io.Source.fromFile(new File(doc_path)).getLines()) {
	       documents(index) = new Document()
	       val tokens = line.split(" ");
	       for (i <- 1 until tokens.length) {
	       	   val id_count_pair = tokens(i).split(":")
		   documents(index).ids.add(id_count_pair(0).toInt)
		   documents(index).counts.add(id_count_pair(1).toInt)
	       }	   
	       index = index + 1
	   }
	   return documents
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

        val sc = new SparkContext(args(0), "AsymUPConvex")
        val path = args(1)
	val num_words = args(2).toInt
	val num_topics = args(3).toInt
        val slices = if (args.length > 2) args(4).toInt else 1

        val chunk_size = 1
	val alpha = 1.0 / num_topics
	val eta = 1.0 / num_topics
    	
	var twd = new TopicWordDistribution(num_topics, num_words)
	twd.unif_init()
	var exp_e_log_beta = new ExpELogBeta(num_topics, num_words)
	LDAHelpers.update_e_log_beta(twd, exp_e_log_beta);
	var sstats = new TopicESStats(num_topics, num_words) 
	
	val documents = OnlineLDADataReader.read(path, 0)
	var converged = false
	var go = true
	var iter = 1

	val rand = new Random(new Date().getTime())	
	val start = new Date().getTime()
	var time = start
        val runtime = 50

	while (go) {
	      var start_index = 0 //rand.nextInt(documents.length)
	      LDAHelpers.sstats(documents, twd, exp_e_log_beta, start_index, chunk_size, sstats, alpha)
	      LDAHelpers.update_topic_word_dist(twd, sstats, iter, documents.length, chunk_size, eta)
	      sstats.clear()
	      LDAHelpers.update_e_log_beta(twd, exp_e_log_beta);
	      var time = new Date().getTime()
	      go = !converged && ((time - start) / 1000 < runtime) && (iter < 100)
	      print("Rajesh"); println(go)
	      iter = iter + 1
	}
	// Write out the topics to file      	
	TicTocLR.appendToFile("OnlineLDA.result", twd.toString)
        sc.stop()
    }
}
