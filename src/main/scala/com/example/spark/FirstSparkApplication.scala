package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object FirstSparkApplication {

  case class Person(name: String, age: Int, sex:String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")
    val sc = new SparkContext(conf)

    simpleParallelizeMultiply(sc)
    printFromArray(sc)
    calculatePi(sc)
    makeParquet()
  }


  private def simpleParallelizeMultiply(sc: SparkContext) = {
    val x = sc.parallelize(Array(1, 2, 3))
    val y = x.map( n => n * n)
    println(y)
  }

  private def makeParquet() = {
    //  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = SparkSession.builder.appName("SampleKMeans")
      .master("local[*]")
      .getOrCreate()
    import sqlContext.implicits._

    val data = Seq(Person("Jack", 25, "M"), Person("Jill", 25, "F"), Person("Jess", 24, "F"))
    val df = data.toDF()

    import org.apache.spark.sql.SaveMode
    df.select("name", "age", "sex").write.partitionBy("sex").mode(
      SaveMode.Append).format("parquet").save("/tmp/person_partitioned/")

    val dfPerson = sqlContext.read.parquet("/tmp/person_partitioned")
    println("Inside makeParquet")
    df.show
  }

  private def calculatePi(sc: SparkContext) = {
    val NUM_SAMPLES = 409600
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"count is ${count}")
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
  }

  private def printFromArray(sc: SparkContext) = {
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))
    rdd1.collect().foreach(println)
  }
}
