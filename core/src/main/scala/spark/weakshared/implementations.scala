package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer




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
            oldVar.value = myValue
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


object LRProgress {
  type G = ArrayBuffer[Double]
  type P = ArrayBuffer[Double]

  class MasterMessage (
    var id:Long, var message: G, @transient theType: P) extends UpdatedProgressMasterMessage[G,P]
  {
    override def toString = "id:" + id.toString + ":" + message.toString
  }

  class Diff (
    var id:Long, @transient message: G, @transient theType: P) extends UpdatedProgressDiff[G,P]
  {
    var myValue = theType
    def update(oldVar : UpdatedProgress[G,P]) = {
        // todo: locking?
        oldVar.value = myValue
    }

    override def toString = "id:" + id.toString + ":" + myValue.toString
  }

  object Modifier extends UpdatedProgressModifier[G,P] {
    def updateLocalDecideSend(oldVar: UpdatedProgress[G,P], message: G) : Boolean = {
        // always send G; no change to local state
        return true
	}
    def zero(initialValue: P) = ArrayBuffer[Double]()

    def masterAggregate (oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressDiff[G,P] = {
        while (UpdatedProgressVars.Z.size < oldVar.value.size) {
            UpdatedProgressVars.Z.append(0)
        }

        var alpha = 1 / UpdatedProgressVars.numIterations
        for(i <- 0 until UpdatedProgressVars.Z.size) { 
            UpdatedProgressVars.Z(i) = UpdatedProgressVars.Z(i) + message(i)
            oldVar.value(i) = alpha * UpdatedProgressVars.Z(i)
        }

        var doSend = true
        UpdatedProgressVars.numIterations += 1

        var diff = new Diff(oldVar.id, message, oldVar.value)
        return diff
    }

    def makeMasterMessage (oldVar: UpdatedProgress[G,P], message: G) : UpdatedProgressMasterMessage[G,P] = {
        return new MasterMessage(oldVar.id, message, ArrayBuffer[Double]())
    }
  }
}
