package spark

import scala.math.random
import java.net.InetAddress
import scala.util.Random
import scala.util.Random._
import scala.collection.mutable._

/*
Todo: This needs to be made threadsafe to work for multiple tasks running on one machine
*/
object LocalRandom {
    val seed = InetAddress.getLocalHost().hashCode()
    val rand = new Random(seed)

    var tempTour = randomCycle(29, rand)


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
