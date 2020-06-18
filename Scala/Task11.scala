
object Task11 {
   def main(args: Array[String]) {
       //Q1
       val birthdays = Map("Umer" -> "16-02-1995", "Sarmad" -> "11-10-1998", "Adam" -> "15-06-1988")
       println(birthdays)
       //println("Enter a name")
       //val name = scala.io.StdIn.readLine()
       val name = "Adam"
       if(birthdays .contains(name)){
           println(s"$name has the birthday : " + birthdays(name))
       }
       
       //Q2
       val cars = Map("Sedan" -> 1500, "SUV" -> 2000, "Pickup" -> 2500, "Minivan" -> 1600, "Van" -> 2400, "Semi" -> 13600, "Bicycle" -> 7, "Motorcycle" -> 110)
       var carList = cars.filter(_._2<5000).map(_._1)
       println(carList)
       
       //Q3
       val numbers = List(10,20,30,40,50,60)
       print(for ( x <- numbers )yield x*2)
       
       //Q4
       randomString(8)
   }
   def randomString(len: Int): String = {
       val rand = new scala.util.Random(System.nanoTime)
       val sb = new StringBuilder(len)
       val ab = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@£$%^&*().,?"
       for (i <- 1 to len) {
           sb.append(ab(rand.nextInt(ab.length)))
       }
       sb.toString
    }
}


