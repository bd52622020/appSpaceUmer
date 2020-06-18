
object Task10 {
   def main(args: Array[String]) {
       //Q1
       val add = (x:Int) => x + 15
       println(add(10))
       val mul = (x:Int,y:Int) => x * y
       println(mul(12,4))
       
       //Q2
       val data = List(19, 65, 57, 39, 152, 639, 121, 44, 90, 190)
       println("Orginal list: " + data) 
       val result = data.filter(x => x % 19 == 0 || x % 13 == 0) 
       println("\nNumbers of the above list divisible by 19 or 13: \n" + result)
       
       //Q3
       val x = List(0,0,1,2,3,4,4,5,6,6,6,7,8,9,4,4)
       val y = x.distinct
       println(y)
   }
}




