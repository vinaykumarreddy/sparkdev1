package com.uhc.data

import org.apache.spark.sql.execution.datasources.csv.TextInputCSVDataSource
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.jetty.server.Authentication.User

case class details(name:String)

class sparkscalamethods() {

  // file loading method

  def fileloading(sc: SparkContext) {
    val filerdd = sc.textFile("")
    val newUsersPair = filerdd.map { t =>
      val p = t.split("\t")
      (p(0).toInt, p(3))
    }

    val lines = filerdd.map(x => x.split("\t")).cache()
    val count = lines.reduce()
    val result = lines.collect()


  }

  def function(sc: SparkContext, throwable: Throwable) {
    try {
      val filterFunction: String => Boolean =
        (s: String) => s.contains("vinay") || s.contains("uhgprovider")
      print(filterFunction)
    } catch {
      case nullPointerException: NullPointerException => print("null pointer exception occured ")

    }

    def trans(sc: SparkContext): Unit = {
      try {
      }
      val users: List[(Int, User)] = Textinput.fromTextfile()

      def joins(): Unit = {
       // val newjoins =


    catch
    {
      case e: ArithmeticException => print("An exception occured ")
    }
  }
}

object sparkscalamethods1 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("uhc")
    val sc = new SparkContext(conf) \


  }




}
