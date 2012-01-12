package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable.Map

trait UpdatedProgressParam[T] extends Serializable {
  def monotonicUpdate(oldVar: UpdatedProgress[T], newT: T) : Boolean
  def zero(initialValue: T): T
}

class UpdatedProgress[T] (
  @transient initialValue: T, param: UpdatedProgressParam[T]) extends Serializable
{
  val id = UpdatedProgressVars.newId
  var value_ = initialValue
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  UpdatedProgressVars.register(this, true)

  def update (term: T) {
    var sendUpdate = updateWithoutSend(term)
    if (sendUpdate) {
        println("new value to send will be " + value_)
        UpdatedProgressObject.sendUpdatedProgress(this)
    }
  }

  def updateValue (v : T) {
    value_ = v
  }

  def updateWithoutSend (term: T) : Boolean = {
    // todo: don't send this, send intermediate object
    return param.monotonicUpdate(this, term)
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

    def sendUpdatedProgress[T](p: UpdatedProgress[T]){
        executor_callback.sendUpdatedProgress(p, executor_driver, task_description)
    }
}





// TODO: The multi-thread support in this is kind of lame; check
// if there's a more intuitive way of doing it right
private object UpdatedProgressVars
{
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, UpdatedProgress[_]]()
  val localVars = Map[Thread, Map[Long, UpdatedProgress[_]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized { lastId += 1; return lastId }
    
  def register(a: UpdatedProgress[_], original: Boolean): Unit = synchronized {
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

  // Add values to the original vars with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[UpdatedProgress[Any]].updateWithoutSend(value)
      }
    }
  }
}
