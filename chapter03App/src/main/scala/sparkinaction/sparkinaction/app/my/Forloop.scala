package sparkinaction.sparkinaction.app.my

object Forloop {
  
  def main(agrs:Array[String]){
    
    for(i <- 1 to 10){
      println(i)
    }
    
    val yj= Set() ++ ({
      for (j <- 10 to 20) yield j
    })
    
    println(yj)
    
    val isEven= (number:Int) => number%2==0
    
    println(isEven(5))
    
    for{
      i <- 1 to 10
      if(i%2==0)
      if(i>5)  
    }
    println(i)
    println(patternMa("test"))
    println(patternMa(2))
    
  }
  
  def  patternMa(str:Any){
    str match{
      case "test" => println("some")
       case 2 => println("some2")
      case _ => println("none")
    }
  }
}
  
