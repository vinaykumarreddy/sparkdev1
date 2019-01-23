import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala._
import scala.collection._
import collection.mutable._
import collection.immutable._


class CSVfile {

  case class employee(empno: String, ename: String, designation: String, manager: String, hire_date: String, sal: Float, deptno: Int)

  case class date(date: String, longitude: String, Time: String, latitude: String)

  def configuration(throwable: Throwable) {

    val conf = new SparkConf().setAppName("csvfile processing").setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val textrdd = sc.textFile("C:\\Users\\lreddy15\\Downloads\\emp_data.csv", 10)
    val emprdd = textrdd.map({ line => line.split("\t") }).cache()



    val col = employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))




  catch
    {

      case _: Throwable => println("An exception occurred")

    }
      def parsedate(line: String, throwable: String){}

//val tra= Set(emprdd)
//    tra.seq(90:Int, "vinay": String, "IT": String, "ABC": String, "10-aug-2015": String,"10000": Float, "1048789": Int)

  }
}
