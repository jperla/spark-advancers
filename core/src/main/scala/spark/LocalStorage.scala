package spark

import scala.math.random
import java.net.InetAddress
import scala.util.Random
import scala.util.Random._
import scala.collection.mutable._
import scala.io.Source
import scala.io.Source._

/*
Todo: This needs to be made threadsafe to work for multiple tasks running on one machine
*/
object LocalRandom {
    val seed = InetAddress.getLocalHost().hashCode()
    val rand = new Random(seed)

    var tempTour = randomCycle(7663, rand)

    var data = ArrayBuffer.empty[Array[Double]]
    for(line <- Source.fromFile("/home/princeton_ram/weakshared/ym7663.tsp").getLines()) {
            var city = new Array[Double](2)
            var node = line.split(' ')
            city(0) = node(1).toDouble
            city(1) = node(2).toDouble
            data += city
    }

    def getData() : ArrayBuffer[Array[Double]] = {
        return data
    }

    def getRandom () : Random = {
        return rand
    }

    def randomCycle (size: Int, rand: Random): ArrayBuffer[Int] =  {
        var randNodes = ArrayBuffer.empty[Int]
        for(i <- 0 until size) {
            randNodes += i
        }
        return rand.shuffle(randNodes)
    }
}
