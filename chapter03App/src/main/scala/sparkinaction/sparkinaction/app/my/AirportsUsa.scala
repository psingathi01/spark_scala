package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AirportsUsa {
  
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("air").setMaster("local")
    
    val sc=new SparkContext(conf)
    
    val lines=sc.textFile("C:\\spark\\data\\airports.txt", 1)
    
    val usaairports=lines.filter(line => {
       line.split(",")(3).equals("\"United States\"")
     }
    ).map(line => {
      
     line.split(",")(1)+" "+ line.split(",")(2)
    })
    
  
   
  for(s <- usaairports.collect()){
    println(s)
  }
    
  
  
  }
}