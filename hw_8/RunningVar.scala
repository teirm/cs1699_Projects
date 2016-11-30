class RunningVar extends java.io.Serializable {
  //declare var here

  var sum = 0.0
  var sum_squares = 0.0
  var count = 0

  def this(sum: Double, sum_squares: Double, count: Int) { 
    this() 
    
    this.sum = sum
    this.sum_squares = sum_squares
    this.count = count
  }

  //Compute initial variance for numbers
  def this(numbers:Iterator[Double]) {
    this()

    this.sum = sum
    this.sum_squares = sum_squares
    this.count = count
   
    var (numbers_squares_1, numbers_squares_2) = numbers.duplicate  
     
    numbers_squares_1.foreach(this.add(_))
    numbers_squares_2.foreach(this.add_squares(_))

    println(sum)
    println(sum_squares)  
  }

  // Update variance for a single value
  def add(value: Double) {
    sum += value  
    count += 1 
  }

  def add_squares(value: Double) {
    sum_squares += value * value 
  }

  def var_calc() =  {
    val variance = (this.sum_squares / this.count) - (this.sum / this.count) * (this.sum / this.count)   
    variance
  }

  def merge(other:RunningVar) = {
    val merged_sum = this.sum + other.sum
    val merged_sum_square = this.sum_squares + other.sum_squares
    val merged_count = this.count + other.count
    
    new RunningVar(merged_sum, merged_sum_square, merged_count) 
  }
}

var r = new scala.util.Random
val doubleRDD = sc.parallelize(Seq.fill(100)(r.nextDouble + r.nextInt(100))) 
val result = doubleRDD.mapPartitions(v => Iterator(new RunningVar(v))).reduce((a,b) => a.merge(b))
val variance = result.var_calc 
System.exit(0)
