package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable.Map

trait UpdatedProgressModifier[G,T] extends Serializable {
  def zero(initialValue: T): T

  def updateLocalDecideSend(oldVar: UpdatedProgress[G,T], message: G) : Boolean
  def masterAggregate (oldVar: UpdatedProgress[G,T], message: G) : UpdatedProgressDiff[G,T]
  def makeMessage (oldVar: UpdatedProgress[G,T], message: G) : UpdatedProgressMasterMessage[G,T]
}

class UpdatedProgressMasterMessage[G,T] (
    var id:Long, var message: G, @transient theType: T) extends Serializable {
}

trait UpdatedProgressDiff[G,T] extends Serializable {
    def update(original : UpdatedProgress[G,T])
    def id : Long
}


// this is asymmetric!
class UpdatedProgress[G,T] (
  @transient initialValue: T, param: UpdatedProgressModifier[G,T]) extends Serializable
{
  val id = UpdatedProgressVars.newId
  var value_ = initialValue
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  UpdatedProgressVars.register(this, true)

  def advance (message: G) = {
    var doSend = param.updateLocalDecideSend(this, message)
    if (doSend) {
        println("new value to send will be " + message)
        var upmm = param.makeMessage(this, message)
        UpdatedProgressObject.sendUpdatedProgressMasterMessage(upmm)
    }

  }

  def masterAggregate (message: G) : UpdatedProgressDiff[G,T] = {
    return param.masterAggregate(this, message)
  }

  /*
  def update (term: T) {
    var sendUpdate = updateWithoutSend(term)
    if (sendUpdate) {
        println("new value to send will be " + value_)
        UpdatedProgressObject.sendUpdatedProgress(this)
    }
  }
  */

  def updateValue (v : T) = {
    value_ = v
  }

  def updateWithoutSend (message: G) : Boolean = {
    // todo: don't send this, send intermediate object
    return param.updateLocalDecideSend(this, message)
  }

  def value = this.value_
  def value_= (t: T) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    // don't initialize to 0 use same one
    //value_ = zero
    deserialized = true
    UpdatedProgressVars.register(this, false)
  }

  override def toString = value_.toString
}


object UpdatedProgressObject {
    var executor_callback : Executor = null
    var executor_driver : ExecutorDriver = null
    var task_description : TaskDescription = null

    //This is wrong. Task description gets overwritten by each task running on a machine.
    //we should fix this when we move to actors
    def setExecutor (e: Executor, d: ExecutorDriver, t: TaskDescription) { 
        executor_callback = e 
        executor_driver = d
        task_description = t
    }

    def sendUpdatedProgressMasterMessage[G,T](p: UpdatedProgressMasterMessage[G,T]){
        executor_callback.sendUpdatedProgressMasterMessage(p, executor_driver, task_description)
    }
}


  class MinDoubleUpdatedProgressMasterMessage (
    id:Long, message: Double, @transient theType: Double) extends UpdatedProgressMasterMessage[Double,Double]
  {
  }

  class MinDoubleUpdatedProgressDiff (
    var id:Long, @transient message: Double, @transient theType: Double) extends UpdatedProgressDiff[Double,Double]
  {
    myValue = message
    def update(oldVar : UpdatedProgress[Double,Double]) = {
        if (myValue < oldVar.value) {
            oldVar.value = myValue
        }
    }
  }

  object MinDoubleUpdatedProgressModifier extends UpdatedProgressModifier[Double,Double] {
    def updateLocalDecideSend(oldVar: UpdatedProgress[Double,Double], message: Double) : Boolean = {
		if (message < oldVar.value) {
            println("old value was " + oldVar.value);
			oldVar.updateValue(newT)
            println("updated value to " + oldVar.value);
            return true
        } else {
            return false
        }
	}
    def zero(initialValue: Double) = Double.PositiveInfinity

    def masterAggregate (oldVar: UpdatedProgress[Double,Double], message: Double) : UpdatedProgressDiff[Double,Double] = {
        var doSend = updateLocalDecideSend(oldVar, message)

        // todo: why am i sending these things, do i have to? 0.0 transient?!
        var diff = MinDoubleUpdatedProgressDiff(oldVar.id, message, 0.0)
        diff.setMyValue(message)
        return diff
    }

    def makeMessage (oldVar: UpdatedProgress[Double,Double], message: Double) : UpdatedProgressMasterMessage[Double,Double] = {
        // todo: why am i sending these things, do i have to? 0.0 transient?!
        return MinDoubleUpdatedProgressMasterMessage(oldVar.id, message, 0.0)
    }
  }



// TODO: The multi-thread support in this is kind of lame; check
// if there's a more intuitive way of doing it right
private object UpdatedProgressVars
{
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, UpdatedProgress[_,_]]()
  val localVars = Map[Thread, Map[Long, UpdatedProgress[_,_]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized { lastId += 1; return lastId }
    
  def register(a: UpdatedProgress[_,_], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val vars = localVars.getOrElseUpdate(Thread.currentThread, Map())
      vars(a.id) = a
    }
  }

  def hasOriginal(id: Long) : Boolean = {
    return originals.contains(id)
  }

  // Clear the local (non-original) vars for the current thread
  def clear: Unit = synchronized { 
    localVars.remove(Thread.currentThread)
  }

  // Get the values of the local vars for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, v) <- localVars.getOrElse(Thread.currentThread, Map()))
      ret(id) = v.value
    return ret
  }

  def applyDiff[G,T] (diff : UpdatedProgressDiff[G,T]) = synchronized {
    var up = localVars(Thread.currentThread)(diff.id).asInstanceOf[UpdatedProgress[G,T]]
    diff.update(up)
  }

  // Add values to the original vars with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[UpdatedProgress[Any,Any]].updateWithoutSend(value)
      }
    }
  }
}
