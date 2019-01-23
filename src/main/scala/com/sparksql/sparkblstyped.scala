package com.sparksql

import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}

case class series(id:Int,area:String,measure:String,title:String)
case class LAdata(id:String,year:Int,period:String,value:Double)

object sparkblstyped {
  def main(args: Array[String]): Unit = {


  val sparksql = SparkSession.builder().master("local").getOrCreate()
  val sQLContext=SQLContext(sparksql)

  sparksql.sparkContext.setLogLevel("WARN")

  //case class for series data,
  // // By reading we can transform as we need


  // to make type data set use as(alias to )
  // use schema(case class)-encoders --dataframes,
  //caseclas extends type class product

  val countydata = sparksql.read.schema(Encoders.product[LAdata].schema)
    .option("header", true).option("delimiter", "\t").csv("").as("LAdataata")

  countydata.show()


  // below is in dataset of string, converting to dataset of series

  val series = sparksql.read.text("data\LAdata.series")
  series.map({ i => series(series("0"),series("1"), series("2"), series("3")}).cache()
  series.show()



}










sparksql.stop()
}
