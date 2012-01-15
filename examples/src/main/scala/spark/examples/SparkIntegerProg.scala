package spark

import java.util.Random
import scala.math.exp
import Vector._
import spark._

object SparkIntegerProg {
  val N = 1000  // Number of chunks
  val D = 10   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5

  
  class KnapsackState (t: Array[Int] = Array.empty[Int], c: Double = -1) extends Serializable {
      var tour = t
      var cost = c
      
      override def toString(): String = {
          var s = new String()
          tour.foreach(p => s += p.toString + ", ")
          return s
      }
  }

  type K = KnapsackState
  object KnapsackStateAccumulatorParam extends AccumulatorParam[K] {
      def addInPlace(t1: K, t2: K): K = {
          if (t1.cost == -1) {
              return t2
          }
          if (t2.cost == -1) {
              return t1
          }

          if (t1.cost > t2.cost) {
              return t1
        } else {
              return t2
          }
      }

      //Todo: this is weird. we dont touch initialValue
      def zero(initialValue: K): K = { return initialValue }
  }


  def main(args: Array[String]) {
    if (args.length < 2) {
        System.err.println("Usage: AsymUPConvex <host> <input> <slices> <numIter>")
        System.exit(1)
    }

    val sc = new SparkContext(args(0), "SparkIntegerProg")
    val path = args(1)
    val slices = if (args.length > 2) args(2).toInt else 2
    val iter = if (args.length > 3) args(3).toInt else 100000

    // get the data into array of array (table)
    val source = Source.fromFile(path)
    val lines = source.mkString.split("\n")
    source.close()
    val table = lines.map(l => l.split(",").map(i => i.toDouble))



    val sizeOfSolution = table(0).size - 1
    println("size of solution: " + sizeOfSolution);
    var lowerBound = 0.0
    var bestGuess = Array[Int]()
    var upperBound = Double.NegativeInfinity

    var oldGuess = Array[Int]()
    var guess = Array[Int](1)
    var i = 0
    while(guess != null && oldGuess != guess) {
        i += 1

        if (guess.size == sizeOfSolution) {
            if (isFeasible(table, guess)) {
              // todo: n squared?
              val score = guess.reduceLeft[Int](_+_)
              if (score > lowerBound) {
                lowerBound = score
                bestGuess = guess.clone()
              }
            }

            oldGuess = guess.clone()
            guess = nextSubtree(sizeOfSolution, guess.clone())
        } else {
          var bound = solveRelaxation(table, guess)
          if (bound < lowerBound) {
            println("skip subtree")
            oldGuess = guess.clone()
            guess = nextSubtree(sizeOfSolution, guess.clone())
          } else {
            if (bound > upperBound) {
                upperBound = bound
            }
            oldGuess = guess.clone()
            guess = nextSolution(sizeOfSolution, guess.clone())
          }
        }

        println("guess: " + arrayToString(guess));
        println("best guess: " + arrayToString(bestGuess));
        println(lowerBound + " < value < " + upperBound);
    }

    println("num iterations: " + i);
    println("guess: " + arrayToString(guess));
    println("best guess: " + arrayToString(bestGuess));
  }


}
