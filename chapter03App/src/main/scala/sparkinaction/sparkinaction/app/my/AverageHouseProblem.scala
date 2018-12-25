package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AverageHouseProblem {
  
    def main(args:Array[String]){
      val conf= new SparkConf().setAppName("house").setMaster("local")
      val sc= new SparkContext(conf)
      
      val housePrices=sc.textFile("C:\\spark\\data\\real_estate.csv", 1);
      val cleanLines=housePrices.filter(line=> !line.contains("Bedrooms"))
      
      val pairBedRoomsPrices=cleanLines.map(line => (line.split(",")(3),(1, line.split(",")(2).toDouble)))
      
      for ((bedroom, total) <- pairBedRoomsPrices.collect()) 
         println(bedroom + " : " + total)
     
      val averagePrices=pairBedRoomsPrices.reduceByKey((k,v)=>
        ( k._1+v._1,k._2+v._2 ) )
      
       println("housePrice: ")
       for ((bedroom, total) <- averagePrices.collect()) 
         println(bedroom + " : " + total)
         
    val housePriceAvg = averagePrices.mapValues(avgCount =>
      avgCount._2 / avgCount._1)
      println("housePriceAvg: ")
    
    for ((bedroom, avg) <- housePriceAvg.collect()) 
      println(bedroom + " : " + avg)


      
      
    }
  
}