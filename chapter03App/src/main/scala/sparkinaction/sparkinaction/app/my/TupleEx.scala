package sparkinaction.sparkinaction.app.my

object TupleEx {
  
  def main(args:Array[String]){
    
    val t2=new Tuple2("t1","t1val")
    println(t2._1)
    println(t2._2)
    
    val t3= new Tuple3("t2","t2val",2)
    
    val l= List(t2,t3)
    
    val result=l.foreach( tuple =>{
      
      tuple match{
        case ("t2","t2val",2) => println("2")
        case _ => println("default")
      }
        
      
    })
    
    for((k,v) <-l){
      println(k+" "+v)
    }
    
    for((k,v,f) <-l){
      println(k+" "+v+" "+f)
    }
    
    val collect=Seq()++(
      for((k,v) <-l) yield (k,v)
    )
    
    println(collect)
  }
  

}