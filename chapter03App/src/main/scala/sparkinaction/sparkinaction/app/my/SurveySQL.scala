package sparkinaction.sparkinaction.app.my
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
 import org.apache.spark.sql.types.IntegerType


object SurveySQL {
  
  def main (args:Array[String]){
    
     val warehouseLocation = "file:///C:/SparkSamplaScala/spark-warehouse"
     val schema=StructType(Array(
      StructField("ts", TimestampType, true),
      StructField("age", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("country", StringType, true),
      StructField("state", StringType, true),
      StructField("self_employed", StringType, true),
      StructField("family_history", StringType, true),
      StructField("treatment", StringType, true),
      StructField("work_interfere", StringType, true),
      StructField("no_employees", StringType, true),
      StructField("remote_work", StringType, true),
      StructField("tech_company", StringType, true),      
      StructField("benefits", StringType, true),
      StructField("care_options", StringType, true),
      StructField("wellness_program", StringType, true),
      StructField("seek_help", StringType, true),
      StructField("anonymity", StringType, true),
      StructField("leave", StringType, true),
      StructField("mental_health_consequence", StringType, true),
      StructField("phys_health_consequence", StringType, true),      
      StructField("coworkers", StringType, true),
      StructField("supervisor", StringType, true),      
      StructField("mental_health_interview", StringType, true),
      StructField("phys_health_interview", StringType, true),
      StructField("mental_vs_physical", StringType, true),
      StructField("obs_consequence", StringType, true),
      StructField("comments", StringType, true)
    ))
    
     val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SurveySQL")
      .config("spark.sql.warehouse.dir", warehouseLocation)
     // .enableHiveSupport()
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
   val df= spark.read .format("csv")
  .option("header", true)
  .schema(schema)
  .option("inferSchema", false).csv("C:/spark/data/survey.csv")
   df.show(10)
   df.printSchema()
   df.createOrReplaceTempView("survey")
   
    val resultsDF = spark.sql("SELECT gender, state FROM survey WHERE age > 30")
    resultsDF.show(10)
     val resultsDF2 = spark.sql("SELECT Gender, treatment,tech_company FROM survey WHERE age > 30")
    resultsDF2.show(10)


  }

}