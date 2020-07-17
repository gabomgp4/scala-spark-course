val number = 3

number match {
  case 1 => println("One")
  case 3 => print("Two")
}

for (x <- 1 to 4) {
  val squared = x * x
  println(squared)
}

type NInt[X] = List[List[X]]

val l = List("ppp")
println(l)


for (x <- 1 to 10) println(x)

def fibonacci(n: Int) = {
  var a = 1
  var b = 1
  println(a)
  println(b)
  for (i <- 1 to n) {
    val temp = b
    b = a + b
    a = b
    println(b)
  }
}

fibonacci(10)