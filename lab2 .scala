object Lab2 {
   
   def upper(mystrings: Vector[String]) = {mystrings.map(x =>x.toUpperCase()) }
   def su(mystrings: Vector[String]) = {mystrings.filter(s => s.contains("su"))    //code in here
   }

   def tot(mystrings: Vector[String]) = {mystrings.foldLeft(0)((result: Int, head: String) => result+head.size)
     //code in here
   }

   def evaluate(r: Node): Double = {
      r match {
        case Node(a, Right(m), b) if a == None => m
        case Node(Some(x), Left(y), Some(z)) if y == "+" => evaluate(x)+evaluate(z)
        case Node(Some(x), Left(y), Some(z)) if y == "*" => evaluate(x)*evaluate(z)
        case Node(Some(x), Left(y), Some(z)) if y == "*" => ((evaluate(x)+evaluate(z))*(evaluate(x)*evaluate(z)))
        case _ => throw new Exception("I don't know") //this case catches everything else
      }

   }

}
