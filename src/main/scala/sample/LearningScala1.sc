val hello = "Hola!"
println(hello)

val num = 6
println(s"The number is: ${num.toString.padTo(5, '-')}")

val theUltimateAnswer = "To life, the universe, and everything is 42"
val pattern = """.* ([\d]+).*""".r
val pattern(answerString) = theUltimateAnswer

def printDouble(a: Double): Unit = {
  println(f"${a*2}%.3f")
}

def testing(a:Double) = {
  a*2
}

printDouble(3.141516)


object Color extends Enumeration {
  type Color = Value
  val White, Gray, Black = Value
}

import scala.math._

Color(min(Color.Black.id, Color.White.id))