package spark

import java.io._


trait WeakSharable[T] extends Serializable {
  var value: T = _
  def monotonicUpdate(newT: WeakSharable[T]): WeakSharable[T]
  def construct(): WeakSharable[T]
}


class DoubleWeakSharable (v: Double = 0.0) extends WeakSharable[Double]
{
	value = v
	/*takes a DoubleWeakSharable and returns a new one with upated value
	*/
	def monotonicUpdate(newT: WeakSharable[Double]): WeakSharable[Double] = 
	{
		/* Returning a new DoubleWeakSharable because unsure of the accumulator behavior
		*/
		var retval = new DoubleWeakSharable()
		if (newT.value > value)
			retval.value = newT.value
		else
			retval.value = value
		return retval
	}
	
	/*Return newly created DoubleWeakSharable
	*/
	def construct(): WeakSharable[Double] = 
	{	
		var retval = new DoubleWeakSharable()
		retval.value = value
		return retval
	}
}

/*
class WeakSharableAccumulatorParam[T] extends AccumulatorParam[WeakSharable[T]] {
    def addInPlace(t1: WeakSharable[T], t2: WeakSharable[T]): WeakSharable[T] = t1.monotonicUpdate(t2)
    def zero(initialValue: WeakSharable[T]): WeakSharable[T] = initialValue.construct()   
}
*/

/*
class WeakShared[T] (
  @transient initialValue: T, param: WeakSharable[T]) extends Serializable
{
	//API
	//def += (term: T) { value_ = param.addInPlace(value_, term) }h
}

*/
