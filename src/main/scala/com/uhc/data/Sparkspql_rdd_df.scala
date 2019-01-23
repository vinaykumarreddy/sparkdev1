package com.uhc.data
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Sparkspql_rdd_df {

  val conf = new SparkConf().setMaster("local").set("spark.app.name","sqlapp")
  val sc=new SparkContext(conf)
  sc.setLogLevel("ERROR")
  case class employee(id:Int,name:String,sal:Float)
  val e1 = new employee(1,"karthik",30000F)
  val e2 = new employee(2,"shekar",24000F)
  val e3 = new employee(3,"geetha",43000F)
  val e4 = new employee(4,"raju",50000F)
  val df_emp = sc.createDataFrame(Seq(e1,e2,e3,e4))
  import Spark.implicits._
  val ds_emp = sc.createDataset(Seq(e1,e2,e3,e4))
  df_emp.printSchema
  ds_emp.printSchema
  df_emp
  val df_emp = spark.createDataFrame(Seq(e1,e2,e3,e4))
  val ds_emp = spark.createDataset(Seq(e1,e2,e3,e4))
  df_emp.collect
  case class employee(id:Int,name:String,sal:Float)
  val e1 = new employee(1,"karthik",30000F)
  val e2 = new employee(2,"shekar",24000F)
  val e3 = new employee(3,"geetha",43000F)
  val e4 = new employee(4,"raju",50000F)
  val df_emp = spark.createDataFrame(Seq(e1,e2,e3,e4))
  import spark.implicits._
  val ds_emp = spark.createDataset(Seq(e1,e2,e3,e4))
  df_emp.printSchema
  ds_emp.printSchema
  df_emp
  val df_emp = spark.createDataFrame(Seq(e1,e2,e3,e4))
  val ds_emp = spark.createDataset(Seq(e1,e2,e3,e4))
  df_emp.collect
  ds_emp.collect
  ds_emp.map(x => x.id).show
  ds_emp.show
  ds_emp.show
  df_emp.show
  ds_emp.show
  ds_emp.show
  ds_emp.map(x => x.name.contains("r")).show
  df_emp.show
  df_emp.select("*").where("sal>30000").show
  ds_emp.select("*").where("sal>30000").show
  ds_emp.select("*").filter("sal>30000").show
  df_emp.select("*").filter("sal>30000").show
  df_emp.show
  df_emp.select("*").where("sal>30000 and id%2==0").show
  ds_emp.select("*").where("sal>30000 and id%2==0").show
  ds_emp.select("*").where("name='r'").show
  ds_emp.select("*").where("name='geetha'").show
  ds_emp.registerTempTable("t1")
  spark.sql("select * from t1 where id in (1,4)").show
  ds_emp.registerTempTable("t1")
  ds_emp.createOrReplaceTempView("t2")
  val df_cancer = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/karna/Desktop/Hive/CancerData10.csv")
  df_cancer.printSchema
  case class Cancer(ID:Int,Name:String,Age:Int,Sex:String,State:String,Symptoms:String,Diagnosis:String,Cancer:String,Stage:String,Treatment:String,Survival:String)
  val ds_cancer = df_cancer.as[Cancer]
  df_cancer.show(5)
  ds_cancer.show(5)
  case class Newc(ID:Int,Name:String,Age:Int)
  val ds2 = df_cancer.select("ID","Name","Age").as[Newc]
  val df2 = df_cancer.select("ID","Name","Age")
  val ds2 = df2.as[Newc]
  df2.show
  val df2 = df_cancer.select("ID","Name","Age")
  val ds2 = df2.as[Newc]
  df_cancer.show(5)
  df_cancer.show(5)
  val d1 = df_cancer
  d1.show(5)
  d1.show(5)
  d1.withColumn("newc",lit(100)).show
  val d2 = d1.withColumn("newc",lit(100))
  d1.printSchema
  d2.printSchema
  val d2 = d1.withColumn("newc",lit(100))
  d1.show(5)
  d1.show(5)
  val d3 = d1.withColumn("newc",col("Cancer")+col("CancerSc"))
  val d3 = d1.withColumn("newc",col("Cancer")+col("CancerSc"))
  d3.show(5)
  val d3 = d1.withColumn("newc",col("Cancer").plus(col("CancerSc")))
  d3.show(5)
  val d3 = d1.withColumn("newc",col("Cancer")+col("CancerSc"))
  d1.show(5)
  d1.show(5)
  import org.apache.spark.sql.functions._
  val d4 = d1.withColumn("gender",when(col("Sex")==="M","1").otherwise ("0"))
  d4.show(5)
  d4.show(20)
  d4.show(50)
  d4.show(500)
  d4.show(1000)
  d4.where("Sex='F'").show(10)
  d4.where("Sex='F'").show(10)
  d4.where("Sex='M'").show(10)
  d1.show(5)
  val d4 = d1.withColumn("gender",when(col("Age")>=18 and col("Age")<=25 , "TeenAge").when (col("Age")>25 and col("Age")<=35, "YoungAge").when(col("Age")>35 and col("Age")<=50,"MiddleAge").otherwise ("OldAge"))
  d4.select("Age","gender").show(20)
  val d5 = d4.withColumnRenamed("gender","AgeGroup")
  d4.printSchema
  d5.printSchema
  val d4 = d1.withColumn("Age",when(col("Age")>=18 and col("Age")<=25 , "TeenAge").when (col("Age")>25 and col("Age")<=35, "YoungAge").when(col("Age")>35 and col("Age")<=50,"MiddleAge").otherwise ("OldAge"))
  d4.show(5)
  d4.show(5)
  d5.printSchema
  val d4 = d1.withColumn("Age",when(col("Age")>=18 and col("Age")<=25 , "TeenAge").when (col("Age")>25 and col("Age")<=35, "YoungAge").when(col("Age")>35 and col("Age")<=50,"MiddleAge").otherwise ("OldAge"))
  d4.printSchema
  d5.printSchema
  val d6 = d5.withColumn("ID",col("ID").cast("String"))
  d6.printSchema
  val d7 = d5.select("Age","Name","ID")
  d7.show(5)
  d7.printSchema
  d7.printSchema
  case class newRows(Age:Int,Name:String,ID:Int)
  val rows = Seq(newRows(25,"kishore",4321),newRows(45,"rajshekar",7653))
  val dfnew = rows.toDF()
  val addnew = d7.unionAll(dfnew)
  val addnew = d7.unionAll(dfnew)
  d7.count
  dfnew.count
  addnew.count
  addnew.show
  addnew.show(10002)


}
