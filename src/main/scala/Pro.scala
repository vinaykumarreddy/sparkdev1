import org.apache.spark.{SparkConf, SparkContext}
import scala._
import org.apache._
import org.apache.spark.SPARK_BRANCH

import org.apache.spark.SPARK_BUILD_DATE

object Pro {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("sparkapp")
    val sc = new SparkContext(conf)
    val x = List{1,2,3,4,5};
    val df=sc.parallelize(x)
    val splitdf=df.randomSplit(Array(1,2,3,4,5))
    val (df1,df2,df3,df4,df5)=(splitdf(0),splitdf(1).foreach(x))
    val somplerdd = sc.parallelize("x")

    )



  }
}