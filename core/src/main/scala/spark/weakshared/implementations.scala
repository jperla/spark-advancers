package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

object MaxDoubleProgress {
  type D = Double

  class MasterMessage (
    var id:Long, var message: D, @transient theType: D) extends UpdatedProgressMasterMessage[D,D]
  {
    override def toString = "id:" + id.toString + ":" + message.toString
  }

  class Diff (
    var id:Long, @transient message: D, @transient theType: D) extends UpdatedProgressDiff[D,D]
  {
    var myValue = message
    def update(oldVar : UpdatedProgress[D,D]) = {
        if (myValue > oldVar.value) {
            oldVar.updateValue(myValue)
        }
    }

    override def toString = "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[D,D] {
    def updateLocalDecideSend(oldVar: UpdatedProgress[D,D], message: D) : Boolean = {
		if (message > oldVar.value) {
            println("old value was " + oldVar.value);
			oldVar.updateValue(message)
            println("updated value to " + oldVar.value);
            return true
        } else {
            return false
        }
	}
    def zero(initialValue: D) = Double.PositiveInfinity

    def masterAggregate (oldVar: UpdatedProgress[D,D], message: D) : UpdatedProgressDiff[D,D] = {
        var doSend = updateLocalDecideSend(oldVar, message)

        // todo: why am i sending these things, do i have to? 0.0 transient?!
        var diff = new Diff(oldVar.id, message, 0.0)
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[D,D], message: D) : UpdatedProgressMasterMessage[D,D] = {
        // todo: why am i sending these things, do i have to? 0.0 transient?!
        return new MasterMessage(oldVar.id, message, 0.0)
    }
  }
}



object MinDoubleProgress {
  type D = Double

  class MasterMessage (
    var id:Long, var message: D, @transient theType: D) extends UpdatedProgressMasterMessage[D,D]
  {
    override def toString = "id:" + id.toString + ":" + message.toString
  }

  class Diff (
    var id:Long, @transient message: D, @transient theType: D) extends UpdatedProgressDiff[D,D]
  {
    var myValue = message
    def update(oldVar : UpdatedProgress[D,D]) = {
        if (myValue < oldVar.value) {
            oldVar.updateValue(myValue)
        }
    }

    override def toString = "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[D,D] {
    def updateLocalDecideSend(oldVar: UpdatedProgress[D,D], message: D) : Boolean = {
		if (message < oldVar.value) {
            println("old value was " + oldVar.value);
			oldVar.updateValue(message)
            println("updated value to " + oldVar.value);
            return true
        } else {
            return false
        }
	}
    def zero(initialValue: D) = Double.PositiveInfinity

    def masterAggregate (oldVar: UpdatedProgress[D,D], message: D) : UpdatedProgressDiff[D,D] = {
        var doSend = updateLocalDecideSend(oldVar, message)

        // todo: why am i sending these things, do i have to? 0.0 transient?!
        var diff = new Diff(oldVar.id, message, 0.0)
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[D,D], message: D) : UpdatedProgressMasterMessage[D,D] = {
        // todo: why am i sending these things, do i have to? 0.0 transient?!
        return new MasterMessage(oldVar.id, message, 0.0)
    }
  }
}









object StringMaxIntMapProgress {
  type M = (String,Int)
  type V = Map[String,Int]

  class MasterMessage (
    var id:Long, var message: M, @transient theType: V) extends UpdatedProgressMasterMessage[M,V]
  {
    override def toString = "id:" + id.toString + ":" + message.toString
  }

  class Diff (
    var id:Long, @transient message: M, @transient theType: Map[String,Int]) extends UpdatedProgressDiff[M,V]
  {
    var myValue = message
    def update(oldVar : UpdatedProgress[M,V]) = {
        val (k,v) = myValue : M 
        if (oldVar.value.getOrElse(k, 0) < v) {
            oldVar.value(k) = v
        }
    }

    override def toString = "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[M,V] {
    def updateLocalDecideSend(oldVar: UpdatedProgress[M,V], message: M) : Boolean = {
        val (k,v) = message : M
        if (oldVar.value.getOrElse(k, 0) < v) {
            println("old value was " + oldVar.value);
            oldVar.value(k) = v
            println("updated value to " + oldVar.value);
            return true
        } else {
            return false
        }
	}
    def zero(initialValue: V) = Map[String,Int]()

    def masterAggregate (oldVar: UpdatedProgress[M,V], message: M) : UpdatedProgressDiff[M,V] = {
        var doSend = updateLocalDecideSend(oldVar, message)

        var diff = new Diff(oldVar.id, message, Map[String,Int]())
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[M,V], message: M) : UpdatedProgressMasterMessage[M,V] = {
        return new MasterMessage(oldVar.id, message, Map[String,Int]())
    }
  }
}

class LRProgressUpdate (
    var position: Array[Double], var converged: Boolean) extends Serializable
{
    override def toString: String = { 
        var out = new String
        for (i <- 0 until position.length){
            out = out + "position: " + position(i).toString + "\n"
        }
        out = out + ":" + converged.toString
        return out
    }
}

object LRProgress {
  type G = Array[Double]
  type P = LRProgressUpdate
    
    
  class MasterMessage (
    var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P]
  {
    override def toString: String = {
        var out = new String
        out = out + "\n"+"id: " + id.toString + "\n"
        for (i <- 0 until message.length){
            out = out + "message: " + message(i).toString + "\n"
        }
        return out
    } 
  }

  class Diff (
    var id:Long, @transient message: G, @transient theType: P) extends UpdatedProgressDiff[G,P]
  {
    var myValue = theType
    def update(oldVar : UpdatedProgress[G,P]) = {
        // todo: locking?
        oldVar.value.converged = myValue.converged
        for (i <- 0 until oldVar.value.position.length){
            oldVar.value.position(i) = myValue.position(i)
        }
    }

    override def toString = "\n" + "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[G,P] {
    val eps = 1e-4
    def updateLocalDecideSend(oldVar: UpdatedProgress[G,P], message: G) : Boolean = {
        // always send G; no change to local state
        return true
	}
    def zero(initialValue: P) = new LRProgressUpdate(Array[Double](initialValue.position.size), false)

    def masterAggregate(oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {
        var theta = oldVar
        while (UpdatedProgressVars.Z.size < theta.value.position.size) {
            UpdatedProgressVars.Z.append(0)
        }
        UpdatedProgressVars.numIterations += 1


        
            println("Z: \n")
            for (i <- 0 until UpdatedProgressVars.Z.length){
                println(UpdatedProgressVars.Z(i))
            }
            println("\nX: \n")
            for (i <- 0 until theta.value.position.length){
                println(theta.value.position(i))
            }
        

        var change = 0.0
        for(i <- 0 until UpdatedProgressVars.Z.length) { 
            UpdatedProgressVars.Z(i)  = UpdatedProgressVars.Z(i) + message(i)

            val oldValue = theta.value.position(i)
            val newValue = -1.0 * (UpdatedProgressVars.Z(i) / (2.0 * UpdatedProgressVars.numIterations)) // The 2 is implicit regularization
            theta.value.position(i) = newValue

            change = change + math.pow((oldValue - newValue), 2)
        }
        
        if (math.pow(change, 0.5) < eps) {
            theta.value.converged = true
        } else{
            theta.value.converged = false
        }
        
        println(UpdatedProgressVars.numIterations)

        var diff = new Diff(theta.id, message, theta.value)
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressMasterMessage[G,P] = {
        return new MasterMessage(oldVar.id, message, 
            new LRProgressUpdate(Array[Double](oldVar.value.position.size), false))
    }
  }
}




