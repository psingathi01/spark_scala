package sparkinaction.sparkinaction.app.my

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

case class Post (commentCount:Option[Int], lastActivityDate:Option[java.sql.Timestamp],
  ownerUserId:Option[Long], body:String, score:Option[Int], creationDate:Option[java.sql.Timestamp],
  viewCount:Option[Int], title:String, tags:String, answerCount:Option[Int],
  acceptedAnswerId:Option[Long], postTypeId:Option[Long], id:Long)
  
object ch5ItalianPosts {
  
  def main (args: Array[String]){
    val spark=SparkSession.builder().appName("Italian Posts").master("local[*]").
    config("spark.sql.warehouse.dir", "file:///C:/spark/sparkinaction")
    .getOrCreate()
    val sc=spark.sparkContext
    val itPostsRows=sc.textFile("file:///C:/github-archive/italianPosts.csv")
   // val itPostsSplit=itPostsRows.map(s=>s.split("~"))
    val itPostsRDD=itPostsRows.map(s=>s.split("~"))
    //itPostsSplit.foreach(s=> s(1))
   val itPostsDFCase = itPostsRows.map(x => stringToPost(x))
   val itPostsDF = spark.createDataFrame(itPostsDFCase)
  
   //itPostsDF.printSchema
  // itPostsDFCase.collect().foreach(println)

   import org.apache.spark.sql.types._
val postSchema = StructType(Seq(
  StructField("commentCount", IntegerType, true),
  StructField("lastActivityDate", TimestampType, true),
  StructField("ownerUserId", LongType, true),
  StructField("body", StringType, true),
  StructField("score", IntegerType, true),
  StructField("creationDate", TimestampType, true),
  StructField("viewCount", IntegerType, true),
  StructField("title", StringType, true),
  StructField("tags", StringType, true),
  StructField("answerCount", IntegerType, true),
  StructField("acceptedAnswerId", LongType, true),
  StructField("postTypeId", LongType, true),
  StructField("id", LongType, false))
  )

     val rowRDD = itPostsRows.map(row => stringToRow(row))
   val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
  itPostsDFStruct.columns
  itPostsDF.printSchema()
 //val cnt=itPostsDF.filter("postTypeId == 1 and (acceptedAnswerId isNull)").count()
  val cnt=itPostsDF.filter(col("postTypeId").===(1).and(col("acceptedAnswerId").isNull)).count()
  println(cnt)
  itPostsDF.filter("postTypeId == 1").orderBy(col("lastActivityDate").desc).limit(10).show
  
//val c=itPostsDF.filter("body".contains("Italiano")).count()
   
  }
  
    
 

   import StringImplicits._
  def stringToPost(row:String):Post = {
  val r = row.split("~")
  Post(r(0).toIntSafe,
    r(1).toTimestampSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toTimestampSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong)
      }

 
  def stringToRow(row:String):Row = {
  val r = row.split("~")
  Row(r(0).toInt,
    r(1).to,
    r(2).toLong,
    r(3),
    r(4).toInt,
    r(5),
    r(6).toInt,
    r(7),
    r(8),
    r(9).toInt,
    r(10).toLong,
    r(11).toLong,
    r(12).toLong)
}
  
}

object StringImplicits {
   implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
   }
}