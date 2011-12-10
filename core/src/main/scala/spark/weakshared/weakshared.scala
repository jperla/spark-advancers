package spark

import java.io._


trait WeakSharable[T] extends Serializable {
  var value: T = _
  def monotonicUpdate(newT: WeakSharable[T])
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

