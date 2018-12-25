package org.sia.chapter03App

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

/**
 * @author ${user.name}
 */
object GitHubDay {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
    //  .appName("The swankiest Spark app ever")
     // .master("local[*]")
      //.config("spark.sql.warehouse.dir", args(0))
      .getOrCreate()

    val sc = spark.sparkContext
    
    val homeDir=System.getenv("HOME")
    val inputPath=homeDir+"sia/github-archive/2015-03-01-0.json"
    val ghLog=spark.read.json(args(0))
    val pushes=ghLog.filter("type = 'PushEvent'")
    
    //pushes.printSchema()
   // println("all events "+ghLog.count())
  //  println("only pushes "+pushes.count())
  //  pushes.show(5)
    
    val grouped=pushes.groupBy("actor.login").count()
   // grouped.show(5)
    
    val ordered=grouped.orderBy(grouped("count").desc)
   // ordered.show(5)
 
    val employees=Set() ++ (
        for{
          line <- fromFile(args(1)).getLines
        }yield line.trim
        )
        
        val bcEmployees=sc.broadcast(employees)
        
        import spark.implicits._
        val isEmp=user => bcEmployees.value.contains(user)
        
        val isEmployee=spark.udf.register("SetContainsUdf", isEmp)
        val filtered=ordered.filter(isEmployee($"login"))
        //filtered.show()
        
        filtered.write.format(args(3)).save(args(2))
    
   /* 
    
    val col = sc.parallelize(0 to 100 by 5)
    val smp = col.sample(true, 4)
    val colCount = col.count
    val smpCount = smp.count

    println("orig count = " + colCount)
    println("sampled count = " + smpCount)*/
  }

}
