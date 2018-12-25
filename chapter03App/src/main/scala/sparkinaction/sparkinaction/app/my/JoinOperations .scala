package sparkinaction.sparkinaction.app.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JoinOperations  {
  
  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
        val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

        val join = ages.join(addresses)
        //println(join.take(5).foreach(f))
       for((k,v) <- join.collect()){
         println(" join " +k +" "+v)
       }
           

        val leftOuterJoin = ages.leftOuterJoin(addresses)
          for((k,v) <- leftOuterJoin.collect()){
         println("L join"+k +" "+v)
       }

        val rightOuterJoin = ages.rightOuterJoin(addresses)
        for((k,v) <- rightOuterJoin.collect()){
         println("R join "+k +" "+v)
       }

        val fullOuterJoin = ages.fullOuterJoin(addresses)
           for((k,v) <- fullOuterJoin.collect()){
         println("F join "+k +" "+v)
       }
    }
  
}