import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Main extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark:SparkSession = SparkSession.builder().master("local").appName("jsonParquet")
    .getOrCreate()
  val list=List(11,1125,1128,1161,1194,
    1205,1248,1384,1312,1370,1402,1404,1422,1439,1490,1491,1492,102,
    4265,129,501)
  val df = spark.read.option("multiline","true").json("./app.json")
    .withColumn("dt_acc",
      when(col("b1_i1_n05").isin(list:_*),col("b2_i1_n10"))
        .when(col("b1_i1_n05")===102 || col("b1_i1_n05")===129,col("b2_i1_n12"))
        .when(col("b1_i1_n05")===129,col("b11_i714_n04"))
        .when(col("b1_i1_n02")===1 || col("b1_i1_n02")===2 && !col("b1_i1_n05").isin(list:_*),col("b2_i2_n10"))
        .when(col("b1_i1_n02")=!=1 && col("b1_i1_n02")=!=2 && !col("b1_i1_n05").isin(list:_*),col("b1_i1_n02"))
    )
    .withColumn("dt_cd",
      when(col("b1_i1_n05")===1952,col("b5_i5_n05"))
        .when(col("b1_i1_n05")===501,col("b11_i1906_n04"))
        .when((col("b1_i1_n02")===1) && !col("b1_i1_n05").isin(list:_*),col("b11_i1906_n04"))
    ).select(col("dt_acc"),col("dt_cd")).write.mode("append").saveAsTable("hiveTable")

}

