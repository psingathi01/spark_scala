package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AirportsUsa2 {
  
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("air").setMaster("local")
    
    val sc=new SparkContext(conf)
    
    val lines=sc.textFile("C:\\spark\\data\\airports.txt", 1)
    
lines.map(line => (line.split(",")(2)+","+(line.split(",")(3)).toUpperCase())).saveAsObjectFile("C:\\spark\\data\\out\\airports_uppercase.text")
    
    //cityC.saveAsObjectFile("C:\\spark\\data\\out\\airports_uppercase.text")
//    println(cityC.take(5))
    //cityC.take(5).foreach(f=> println(f._1+" "+f._2))
      //cityC.map(f ->   f._1+","+f._2 ).saveAsTextFile("file:///home/charan/offlinefiles/result");

   // cityC.foreach()
    
  
  
  }
}