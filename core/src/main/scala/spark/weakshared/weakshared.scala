package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._

trait WeakSharable[T] extends Serializable {
  var value: T = _
  def monotonicUpdate(newT: WeakSharable[T])
}


object WeakShared {
    var executor_callback : Executor = null
    var executor_driver : ExecutorDriver = null
    var task_description : TaskDescription = null
    var ws : DoubleWeakSharable = null

    //This is wrong. Task description gets overwritten by each task running on a machine.
    //we should fix this when we move to actors
    def setExecutor (e: Executor, d: ExecutorDriver, t: TaskDescription) { 
        executor_callback = e 
        executor_driver = d
        task_description = t
    }

    def sendWeakShared[T](w: WeakSharable[T]){
        executor_callback.sendWeakShared(w, executor_driver, task_description)
    }

}


class DoubleWeakSharable (v: Double = 0.0) extends WeakSharable[Double]
{
	value = v
	
    def monotonicUpdate(newT: WeakSharable[Double]) = 
	{
		if (newT.value > value)
			value = newT.value
	}
}

