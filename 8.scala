import scala.io.StdIn
import scala.collection.mutable.ArrayBuffer

object MinMax
{
	def main(args: Array[String]): Unit = {
		var numArray = new ArrayBuffer[Int]()
		print("Enter number of elements: ")
		val n = scala.io.StdIn.readInt() //read no. of itms in array
		println("Enter elements below, one per line: ")
		for(i <- 0 until n) //read array elements
			numArray += scala.io.StdIn.readInt()

		//println(numArray) //displaying the elements		
					
		val t = minmax(numArray) //returned value will be a tuple
		printf("Min no. is %d.\n", t._1) //display max value
		printf("Max no. is %d.\n", t._2) //display min value
	}
	
	def minmax(numArray:ArrayBuffer[Int]): (Int,Int) = {
		var mi:Int = numArray(0) //initialize min and max
		var mx:Int = numArray(0)
		
		for(i <- numArray)
		{
			if(i < mi)
				mi = i
			else if(i > mx)
				mx = i
		}
		(mi,mx) //return mx and mi as items of tuple
	}
}
