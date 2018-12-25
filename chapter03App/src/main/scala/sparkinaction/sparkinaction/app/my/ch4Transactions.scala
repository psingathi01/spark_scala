package sparkinaction.sparkinaction.app.my

import org.apache.spark.sql.SparkSession

object ch4Transactions {
  
  def main (args: Array[String]){
      val spark = SparkSession.builder()
      .appName("The swankiest Spark app ever")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/spark/sparkinaction")
      .getOrCreate()

    val sc = spark.sparkContext
    val tranFile=sc.textFile("file:///C:/github-archive/ch04_data_transactions.txt")
     val tranData=tranFile.map(_.split("#"))
      val transByCust=tranData.map(tran => (tran(2).toInt,tran))
      //transByCust.values.foreach(s => println(s.mkString(" , ")))
      //transByCust.foreach(f)
   //   transByCust.countByKey().toSeq.foreach(println)
      val(cid,purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
      println(cid)
      println(purch)
   //   transByCust.lookup(cid).foreach(s => println(s.mkString(" , ")))
    var complTrans=Array(Array("2015-03-30","11:59 PM","53","4","1","0.00"))
    
     val transByCust2 = transByCust.mapValues(tran => { 
          if(tran(3).toInt==25 && tran(4).toDouble>1) {
	          tran(5)=(tran(5).toDouble * 0.95).toString
	          println("cid="+tran(2))
          }
         tran})
         
        // transByCust2.collect();
         
           val transByCust3 = transByCust.flatMapValues(tran => { 
          if(tran(3).toInt==81 && tran(4).toDouble>=5) {
	          val cloned=tran.clone()
	           println("cid="+tran(2))
            cloned(5)="0.00";cloned(3)="70";cloned(4)="1";
            List(tran,cloned)
          }else {
            List(tran)
          }
         })
         transByCust3.collect();
      
      //transByCust3.lookup(77).foreach(s => println(s.mkString(" , ")))
      val amounts =transByCust.mapValues(t => t(5).toDouble)
      val totals=amounts.foldByKey(0)((p1,p2)=> p1+p2).collect()
      val amt = totals.toSeq.sortBy(_._2).last
     //println(amt)
      //amounts.foreach(println)
     // amounts.foreach(s => println(s.mkString(" , ")))
      
       val totals2=amounts.foldByKey(100000)((p1,p2)=> p1+p2).collect()
       totals2.foreach(println)  
  }
  
}