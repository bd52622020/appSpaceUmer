
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.Date
import java.text.SimpleDateFormat

object movieLens {
    def genreName(data :String)={
        val fields = data.split('|')
        (fields(1).toInt,fields(0).toString)
    }    
    def usrZipcode(data:String) = {
        val fields = data.split('|')
        (fields(4), fields(0).toInt)
    }

    def usrZipcode2(data:String) = {
        val fields = data.split('|')
        (fields(0).toInt, fields(4))
    } 

    def loadMovieNames(line:String)={
        val fields = line.split('|')
        (fields(0).toInt,fields(1).toString)
    }
    def ratingMovie(line:String) ={
        val fields = line.split('\t')
        (fields(1).toInt,fields(2).toFloat)
    }
    def occupation(line:String) ={
        val fields = line.split('|')
        (fields(0).toInt, fields(3).toString)
    }
    def usrRating(data:String) = {
        val fields = data.split('\t')
        (fields(0).toInt,(fields(1).toInt,fields(2).toInt))
    }

    def ratingDate(data :String) = {
        val fields = data.split('\t')
        val unix_seconds = fields(3).toLong
       //convert seconds to milliseconds
        val date= new Date(unix_seconds*1000L); 
        val jdf = new SimpleDateFormat("yyyy")
        val dd = jdf.format(date)
        (fields(1).toInt,dd.toInt)
    }

    def movieRelease(data:String) = {
        val fields = data.split('|')
        val date = fields(2).toString
        if(date != ""){
            val jdf = new SimpleDateFormat("dd-MMM-yyyy")
            val dd = jdf.parse(date)
            (fields(0).toInt,dd)
        }
        else{
            val jdf = new SimpleDateFormat("dd-MMM-yyyy")
            val dd = jdf.parse("00-Jan-2020")
            (fields(0).toInt,dd)
        }
    }
    def usrAge(data:String) = {
        val fields = data.split('|')
        (fields(0).toInt,fields(1).toInt)
    } 
    def getGenre(data2:String)={
        val x = data2.split('|')
        (x(0).toInt,x(5).toInt,x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt,x(10).toInt,x(11).toInt,x(12).toInt,x(13).toInt,x(14).toInt,x(15).toInt,x(16).toInt,x(17).toInt,x(18).toInt,x(19).toInt,x(20).toInt,x(21).toInt,x(22).toInt,x(23).toInt)
    }
    def main(args: Array[String]) {
        //Load up the raw u.data file
        val lines = sc.textFile("u.data")
        //Convert to (movieID, (rating, 1.0))
        val movieRatings = lines.map(ratingMovie)
        //get occupation
        val user = sc.textFile("u.user")
        val occuData = user.map(occupation)
        //get userid mid and rating
        val usrMvRt = lines.map(usrRating)
        //getting mvID and mvNames
        val movieTxt = sc.textFile("u.item")
        val movieNames = movieTxt.map(loadMovieNames).collectAsMap 
        
        //Q top 10 rated movies sorted by rating
        val ratingCount = movieRatings.map(x=> (x._1,(x._2,1)))
                                   .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                                   .mapValues(x => x._2)
                                   .sortBy(x => x._2,false).take(10)
        for (res <- ratingCount){
            println( movieNames(res._1) +" " + res._2)
        }
        
        //QMost Rated Movie by Students
        //sorted by user id
        val sortedusrMvRt = usrMvRt.sortBy(x=>x._1)
        sortedusrMvRt.first
        //only getting user that are stuent
        val student = occuData.filter(x => x._2 == "student")
        student.first
        val stuRating = sortedusrMvRt.join(student)
        stuRating.first
        val stuRating2 = stuRating.mapValues(x=>x._1)
                                  .map(x=>x._2)
                                  .map(x=> (x._1,(x._2,1)))
                                  .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                                  .mapValues(x => x._2)
                                  .sortBy(x => x._2,false).take(1)
        for (res <- stuRating2){
            println( movieNames(res._1) +" " + res._2)
        }
        
        //Q movies that were rated after 1960
        val dateRating = lines.map(ratingDate)
        dateRating.first
        val ratedAfter1960 = dateRating.filter(x=>x._2 > 1960).take(20) 
        for(res <- ratedAfter1960){
            if(res._2 == 1960){
                print("ERROR")
            }
            else{
                println( movieNames(res._1) +" " + res._2)
            }
        }
        
        //Q Most Rated movie by 20-25
        val usrLife = user.map(usrAge)
        val setAge = usrLife.filter(x=>(x._2 >= 20 && x._2 <= 25))
        val ageRating = sortedusrMvRt.join(setAge)
        val ageRating2 = ageRating.mapValues(x=>x._1)
                                  .map(x=>x._2)
                                  .map(x=> (x._1,(x._2,1)))
                                  .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                                  .mapValues(x => x._2)
                                  .sortBy(x => x._2,false).take(1)
        for (res <- ageRating2){
            println( movieNames(res._1) +" " + res._2)
        }
        
        //top movies rated 5 
        val mvRated5 = movieRatings.filter(x=>x._2 == 5.0)
                                   .map(x=> (x._1,(x._2,1)))
                                   .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                                   .mapValues(x => x._2)
                                   .sortBy(x => x._2,false).take(10)
        for (res <- mvRated5){
            println( movieNames(res._1) +" " + res._2)
        }
        
        //top ten zipcode with most rated movies
        val usrZip = user.map(usrZipcode)
        val topZips= usrZip.map(x=> (x._1,(x._2,1)))
                           .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                           .mapValues(x => x._2)
                           .sortBy(x => x._2,false).take(10).toMap
        //getting user of top ten zipcode 
        def getZipT(x : String): String ={
            if(topZips.contains(x)){
                return(x)
            }
            else{
                ""
            }
        }
        val usrZip2 = user.map(usrZipcode2)
        val usrZip3 = usrZip2.filter(x=>x._2 == getZipT(x._2))
        val mstZipUsr = sortedusrMvRt.join(usrZip3)
        val mstZipUsr2 = mstZipUsr.mapValues(x=>x._1)
                                  .map(x=>x._2)
                                  .map(x=> (x._1,(x._2,1)))
                                  .reduceByKey((x, y) => ( x._1 + y._1, x._2 + y._2 ))
                                  .mapValues(x => x._2)
                                  .sortBy(x => x._2,false).take(10)
        for (res <- mstZipUsr2){
            println( movieNames(res._1) +" " + res._2)
        }
        
        //Q oldest movie rated 5
        val ratMv = lines.map(ratingMovie)
        //movies only rated 5
        val ratMv2 = ratMv.filter(x=>x._2 == 5.0)
        val release = sc.textFile("u.item")
        val relMv = release.map(movieRelease)
        val oldest = relMv.sortBy(x=>x._2).take(1).toMap
        def getMvT(x : Int): Int={
            if(oldest.contains(x)){
                    return(x)
            }
            else{
                0
            }
        }
        val ratMv3 = ratMv2.filter(x=>x._1 == getMvT(x._1)).take(1)
        for(result <- ratMv3){
            println(movieNames(result._1)+ " " + result._2)
        }
        
        //Q Genres of top rated movies
        val mvGenre = movieTxt.map(getGenre)
        mvGenre.take(10)
        val grnTxt = sc.textFile("u.genre")
        val grnName = grnTxt.map(genreName).collectAsMap

        val topTen = ratingCount.toMap
        def getGrenes2(x : (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): List[(Int,String,Int)] ={
            var a : List[(Int,String,Int)] = List()
            if(topTen.contains(x._1)){
                if (x._2 == 1){
                    a= a:+(x._1,grnName(0),topTen(x._1))
                }
                if (x._3 == 1){
                    a= a:+(x._1,grnName(1),topTen(x._1))
                }
                if (x._4 == 1){
                    a= a:+(x._1,grnName(2),topTen(x._1))
                }
                if (x._5 == 1){
                    a= a:+(x._1,grnName(3),topTen(x._1))
                }
                if (x._6 == 1){
                    a= a:+(x._1,grnName(4),topTen(x._1))
                }
                if (x._7 == 1){
                    a= a:+(x._1,grnName(5),topTen(x._1))
                }
                if (x._8 == 1){
                    a= a:+(x._1,grnName(6),topTen(x._1))
                }
                if (x._9 == 1){
                    a= a:+(x._1,grnName(7),topTen(x._1))
                }
                if (x._10 == 1){
                    a= a:+(x._1,grnName(8),topTen(x._1))
                }
                if (x._11 == 1){
                    a= a:+(x._1,grnName(9),topTen(x._1))
                }
                if (x._12 == 1){
                    a= a:+(x._1,grnName(10),topTen(x._1))
                }
                if (x._13 == 1){
                    a= a:+(x._1,grnName(11),topTen(x._1))
                }
                if (x._14 == 1){
                    a= a:+(x._1,grnName(12),topTen(x._1))
                }
                if (x._15 == 1){
                    a= a:+(x._1,grnName(13),topTen(x._1))
                }
                if (x._16 == 1){
                    a= a:+(x._1,grnName(14),topTen(x._1))
                }
                if (x._17 == 1){
                    a= a:+(x._1,grnName(15),topTen(x._1))
                }
                if (x._18 == 1){
                    a= a:+(x._1,grnName(16),topTen(x._1))
                }
                if (x._19 == 1){
                    a= a:+(x._1,grnName(17),topTen(x._1))
                }
                if (x._20 == 1){
                    a= a:+(x._1,grnName(18),topTen(x._1))
                }
            }
            return a
        }

        val movieWithGenre = mvGenre.map(getGrenes2)
        val temp = movieWithGenre.flatMap(x=>x)
        val res = temp.collect
        for(t <- res){
            println(movieNames(t._1) + " "+ t._2 + " " + t._3)
        }
        
        def getGrenes3(x : (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): List[(Int,String)] ={
            var a : List[(Int,String)] = List()
            if(x._1 != 0){
                if (x._2 == 1){
                    a= a:+(x._1,grnName(0))
                }
                if (x._3 == 1){
                    a= a:+(x._1,grnName(1))
                }
                if (x._4 == 1){
                    a= a:+(x._1,grnName(2))
                }
                if (x._5 == 1){
                    a= a:+(x._1,grnName(3))
                }
                if (x._6 == 1){
                    a= a:+(x._1,grnName(4))
                }
                if (x._7 == 1){
                    a= a:+(x._1,grnName(5))
                }
                if (x._8 == 1){
                    a= a:+(x._1,grnName(6))
                }
                if (x._9 == 1){
                    a= a:+(x._1,grnName(7))
                }
                if (x._10 == 1){
                    a= a:+(x._1,grnName(8))
                }
                if (x._11 == 1){
                    a= a:+(x._1,grnName(9))
                }
                if (x._12 == 1){
                    a= a:+(x._1,grnName(10))
                }
                if (x._13 == 1){
                    a= a:+(x._1,grnName(11))
                }
                if (x._14 == 1){
                    a= a:+(x._1,grnName(12))
                }
                if (x._15 == 1){
                    a= a:+(x._1,grnName(13))
                }
                if (x._16 == 1){
                    a= a:+(x._1,grnName(14))
                }
                if (x._17 == 1){
                    a= a:+(x._1,grnName(15))
                }
                if (x._18 == 1){
                    a= a:+(x._1,grnName(16))
                }
                if (x._19 == 1){
                    a= a:+(x._1,grnName(17))
                }
                if (x._20 == 1){
                    a= a:+(x._1,grnName(18))
                }
            }
            return a
        }
        val movieWithGenre2 = mvGenre.map(getGrenes3)
        val mv = movieWithGenre2.flatMap(x=>x)
        val genreRatings = movieRatings.join(mv)
        val genreRatings2 = genreRatings.map(x=>x._2)
                                  .map(x=> (x._2,(x._1,1)))
                                  .reduceByKey((x, y) => ( (x._1+y._1,x._2+y._2)))
                                  .mapValues(x => x._2)
                                  .sortBy(x => x._2,false).take(10)
        //top rated movie by student
        for (result <- genreRatings2){
            println( result._1 +" "+result._2)
        }
    }
}


