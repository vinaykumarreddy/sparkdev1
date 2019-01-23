package com.uhc.data

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.immutable.Range

object InferredschemaExplicitschema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").set("spark.app.name","sqlapp")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val load_rdd=sc.textFile("")

    // loaded list of strings encoded in textfile as string, eventhough seperated by ", " bcoz
    // we loaded as textfile ranther than CSV
    load_rdd.collect()

    // Use map function on rdd to split the data
    val data_split =load_rdd.map(x=>x.split(",") )

    data_split.collect()
      // the data is on 2 D [][] array now, so use somemore transformation by splitting using map() with index
    val data_split2 =data_split.map(x=>x.Row('employee_name' = Stringa[0])),
      'id' = Int(b[1]),
    'age'=scala.Int(b[2]),
      'phone' =Int(c[2]))
data_split2.collect()
  // after collect function the data will be loaded as Row formatted data
    // create a datafram and see what type are infered for those columns

    val dataframe_df=data_split.createDataFrame(data_split2)
    val temptable= load_rdd.createtemoview("details")




    /// Explicit schema declared bcoz all time infered schema is not optimal

    import StructField._
    import org.apache.spark.sql.types.StructField

// below fields are represented as list of struct field types
    val field_schems =  [StructField('id' ,Int[],true),
                        [StructField('age' ,Integer[],true),
                        [StructField('phone' ,StructType[],true),
                        [StructField('location' ,StructType[],true)]

val explicit_schems= StructType(field_schems)

// Now we can pass the explicit schema ref to dataframe
val new_dataframe=Spark.createDataFrame(explicit_schems)

    new_dataframe.columns()
    new_dataframe.schema()


















    val schema = StructField(Array(StructType("desc",String,true), StructField("id",Int,true)))


  }

}
