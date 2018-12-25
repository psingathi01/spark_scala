package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {
  
  def main(args:Array[String]){
    
    val spark=SparkSession.builder().appName("wcount").master("local").getOrCreate()
    
    val sc=spark.sparkContext
    
    val lines=sc.textFile("C:/myspark_java/src/main/resources/input.txt", 1)
    
    val line=lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a,b)=>a+b)
    
    val wcount=line.collect()
    for((w,c) <- wcount){
      println(w +" "+ c)
    }
    
  }
}