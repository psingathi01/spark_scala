package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FilterRdd {
  
  def main (args : Array[String]){
    val conf= new SparkConf().setAppName("filter").setMaster("local")
    val sc= new SparkContext(conf);
    
    val number = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 1)
    
    val num=number.filter(n=> n%3==0)
    
    num.foreach(println)
    
  }
  
}