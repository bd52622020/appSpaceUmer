
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._

object Task13 {
   def main(args: Array[String]) {
       val txt = spark.read.format("csv").option("header","true").load("titanic.csv")
       val srcDF = txt.withColumn("Parents/Children Aboard",col("Parents/Children Aboard").cast(IntegerType)).withColumn("Siblings/Spouses Aboard",col("Siblings/Spouses Aboard").cast(IntegerType)).withColumn("Age",col("Age").cast(IntegerType)).withColumn("Survived",col("Survived").cast(IntegerType)).withColumn("Pclass",col("Pclass").cast(IntegerType))
       srcDF.printSchema
       srcDF.show()
       val Q1 = srcDF.groupBy("Sex","Age","Pclass").agg(count($"Survived"))
       Q1.show
       val Q2 = srcDF.select("Sex","Siblings/Spouses Aboard","Parents/Children Aboard").filter($"Age" < 18).groupBy("Sex").count()
       Q2.show()
       val Q3 = srcDF.select("Sex","Siblings/Spouses Aboard","Parents/Children Aboard").filter($"Age" > 18).groupBy("Sex").count()
       Q3.show()
       val Q4 = srcDF.withColumn("Age", when($"Age" >  0 and $"Age" <= 18, lit("1-18"))
                                 .when($"Age" > 18 and $"Age" <= 35, lit("19-35"))
                                 .when($"Age" > 35 and $"Age" <= 55, lit("36-55"))
                                 .when($"Age" > 55 and $"Age" <= 75, lit("56-75"))
                                 .otherwise(lit("others"))).groupBy("Age").agg(count($"Survived")).orderBy("Age").show
   }
}


