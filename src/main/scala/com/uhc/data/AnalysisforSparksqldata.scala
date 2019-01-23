package com.uhc.data

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._


object AnalysisforSparksqldata {

  val sparksql = SparkSession.builder().master("local").getOrCreate())
  val sQLContext=SQLContext(sparksql)


  //loading particular file format and specifying the header and loading the file
  val loadingdata_Df=sparksql.read.format("").option("header","true").load("loadingdata")

  // preview the columns for loaded file
  loadingdata_Df.columns()

  //if we dont want all columns then we can use Drop() function,here you can mention the respective columns
  val loadiningdata=loadingdata_Df.columns.drop()



  // if we dont want some Row in the file the we can use Filters() function
  // i.e if there are any null values in Rows
  val loadiningdata =loadingdata_Df.columns.filter().column.gender [=Nil]



  // Now create a Temporary "View" valid only for current session
  // loadindata_df = main df


  val tempdata=loadingdata_Df.createGlobalTempView("details")

  val sql = loadingdata_Df('select * from details')




  // Loading another Dataset and later we can join the dataset or columns based on column names


  val power_Df=sparksql.read.format("").option("header","true").load("power")
power_Df.columns

  // it will show the list of columns , so based on same column we can join the dataset results
  // first clean the data  by using Filter () options
  val cleaning=power_Df.filter(power_Df.distinct())

  // we can drop the columns which we dont require

  val removingcolumns=power_Df.columns.drop()

  // Some the Dataframes are in String format to and those are Int format ,
  // so inorder to complete we need to Convert to Int using "Cast"
  removingcolumns=power_Df.withColumn("intelligenct", power_Df["Intelligence"].asInstanceOf[Integer]()).withColumn("height", power_Df["height"].asInstanceOf[Integer])
      .withColumn("city", power_Df["city"].asInstanceOf[Integer]))

  //Transformation has completed , so create Tempview Table
  val temptable=power_Df.createGlobalTempView("powertable")
 val sql=power_Df('select * from powertable');
  power_Df.show(5)


  // write sql queries to analyse the data

  val power_heros=power_Df('select intelligenct,height from power_Df where intelligence >=30 and height >5)
  power_Df.show(10)
  power_Df.orderBy(power_heros,power_heros.desc).show(50)

  // once the query is executed the result is in the form of dataframe which contains 1 row and 1 column  ,
  // to extract number value make use of collecct function wiht index value[]

  val power_heros_count: Int = power_Df.collect.count();

  val intelligence_alignment_agg :String = sparksql('select height from power_df
  ).groupBy().agg().withcolumnrename()


  // join the two dataset






}
