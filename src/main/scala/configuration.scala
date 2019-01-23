import org.apache.spark.{SparkConf, SparkContext}



    case class Id(Code:String,text:String)
    case class columnheaders(id: String, name:String, dept: String,Address: String)

object configuration {
  def main(args: Array[String]): Unit = {
    def LL() {
      throw Exception
    }

    try {
      val conf = new SparkConf().setMaster("local").setAppName("sparkapp")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
      sc.setLogLevel("ERROR")


      val data = Array(1, 2, 3, 4, 5)
      val distData = sc.parallelize(data)
      val count = data.
        map { x =>

        }


    } catch {
      case _: Throwable => println("An exception occurred")
    }


  }
}


