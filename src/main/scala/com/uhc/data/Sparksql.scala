package com.uhc.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql._
object Sparksql {

  object sparkblstyped {
    def main(args: Array[String]): Unit = {

val conf=new SparkConf().setAppName("uhc").setMaster("local")
      val sc=new SparkContext(sc)

      val  movie_record_rdd=sc.parallelize(Row("certificate_id"=1234,
        "movie_name"="ironman",
        "hit"=true,
        "category"=['thriller'],
      "rating"= {"imdb": 6.9,"rotuine tomatos"",7}"releasedate"=datetime (2017,5,3,11,5))]


        // converting Rdd to DF
        val movie_record_rdd_Df = movie_record_rdd.toDF ()
        val movie_record_rdd_Df.show ()

        val movie_record_rdd_Df1 =movie_record_rdd_Df.createorReplaceTempview("records") // records is temp table name valid only for theis session

val movie_record_rdd_Df1=SQLContext('select * from records')

        movie_record_rdd_Df1.show()
        SQLContext('select certificate_id,rating from records where rating["imdb"]<6').show() // sql query


        }
    }
  }
        }



