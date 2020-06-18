
object Task8 {
   def main(args: Array[String]) {
       //Q1
       val dic1=Map(1 -> 10, 2 -> 20)  
       val dic2=Map(3 -> 30, 4 -> 40)  
       val dic3=Map(5 -> 50, 6 -> 60)  
       val dic4 = dic1 ++ dic2 ++ dic3
       println(dic4)
       
       //Q2
       val dic = Map(1 -> 10, 2 -> 20, 3 -> 30, 4 -> 40, 5 -> 50, 6 -> 60)
       var check = (x:Int) => dic.exists(_._1 == x)
       println(check(1))
       println(check(8))
       
       //Q3
       val a= 5
       val b =6
       val c =4
       if((a + b <= c) || (a + c <= b) || (b + c <= a)){

           println(false)
       }
       else{
           print(true)
       }
   }
}


