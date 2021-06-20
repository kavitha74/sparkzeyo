package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkObj {
  
  	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
									val data = sc.textFile("file:///C:/data/txns")
				val mapsplit= data.map(x=>x.split(","))

				val rowrdd = mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val schema_struct = StructType(Array(
						StructField("txnno",StringType,true),
						StructField("txndate",StringType,true),
						StructField("custno",StringType,true),
						StructField("amount", StringType, true),
						StructField("category", StringType, true),
						StructField("product", StringType, true),
						StructField("city", StringType, true),
						StructField("state", StringType, true),
						StructField("spendby", StringType, true)
						))

				val df = spark.createDataFrame(rowrdd, schema_struct)
				df.show()
				df.createOrReplaceTempView("txndf")
				val cashdf = spark.sql("select * from txndf where txnno>40000 and product like '%Gymnastics%'")
				cashdf.show()
					
  	}
}