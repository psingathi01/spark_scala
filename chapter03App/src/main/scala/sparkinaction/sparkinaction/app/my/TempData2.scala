package sparkinaction.sparkinaction.app.my

import scala.io.Source

case class TempData23(day:Int,jd:Int,month:Int,year:Int,prcp:Double,snow: Double,tave:Double,tmax:Double,tmin:Double)
object TempData2 {
  
  def toDoubleOrNeg(s:String):Double ={
    try{
      s.toDouble
    }catch{
      case _:NumberFormatException => -1 
    }
  }
  
  def main(args:Array[String]){
    val fileData=Source.fromFile("C:\\spark\\data\\MN212142_9392.csv")
    val lines=fileData.getLines().drop(1)
    val data=lines.flatMap { line =>
       val p=line.split(",")
        if(p(7)=="." || p(8)=="." || p(9)==".") Seq.empty else
       Seq(TempData23(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, 
          p(9).toDouble))        
    }.toArray
     fileData.close()
   // data.foreach(a=> println(a.year))
    
    val gr=data.groupBy(_.month)
    // gr.foreach(a=> println(a._2))
    val maxTemp=data.map(_.tmax).max
    
    val maxTempDays=data.filter(_.tmax==maxTemp)
    println(s"hot days are ${maxTempDays.mkString(",")}")
    
    val maxTempDayCnt=data.count(_.tmax>=maxTemp)
    println(s" hot days ${maxTempDayCnt}")
    
    val hotdayReduce=data.reduceLeft((d1,d2)=> if(d1.tmax>=d2.tmax) d1 else d2)
    
    println(s"hot day reduce ${hotdayReduce}")
    
    val raindayCount=data.count(_.prcp>=1.0)
    println(s"rainy days count $raindayCount")
    
    val (rainySum,rainyCount)=data.par.aggregate(0.0,0)({
      case ((sum,count),td) => if(td.prcp<1.0) (sum,count) else (sum+td.tmax,count+1)
    }, {
      case ((s1,c1),(s2,c2)) =>
        (s1+s2,c1+c2)
    })
    
    println("average rainy temp "+rainySum/rainyCount)
    
    val monthGroup=data.groupBy(_.month)
    val monthlyAverage=monthGroup.map{ case (m,days)=>
      m -> days.foldLeft(0.0)((sum,td) => sum + td.tmax)/days.length
    }
    monthlyAverage.toSeq.sortBy(_._1) foreach println
    
    
    
    
    
  }
  
}