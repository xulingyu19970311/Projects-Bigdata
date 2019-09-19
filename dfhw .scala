import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
spark.conf.set("spark.sql.crossJoin.enabled", true)// set the crossjoin as enable
val session = SparkSession.builder().getOrCreate()
import session.implicits._
object DataFrameHW{ 
 def trangles(spark: SparkSession): Long={
  //first input the file as csv, rename new comlumn name as E1, E2
  val data = "/ds410/facebook/*.edges"
  val df = spark.read.format("csv").option("sep","\t").load(data)
  val rename = Seq("Edge1","Edge2")
  val df1 = df.toDF(rename: _*)
  
  // Here we calculate the A->B
  val first_trans = dff.as("df1").join(df1.as("df2"),$"df1.Edge2" === $"df2.Edge1")
  val rename1 = Seq("EdA1","EdA2","EdB1","EdB2")
  val dff2 =first_trans.toDF(rename1: _*)
  
  // Here we calculate the B->C
  val second_trans = dff2.as("df1").join(df1.as("df2"),$"df1.EdB2" === $"df2.Edge1")
  val rename2 = Seq("EdA1","EdA2","EdB1","EdB2","EdC1","EdC2")
  val dff3 =second_trans.toDF(rename2: _*)
  
  //Make the filter to make sure A!=B!=C, A=C
  val dff_final= dff3.as("dfff").filter($"EdA1" === $"EdC2" && $"EdA1" != $"EdC1" && $"EdA1"!= $"EdA2")
  dff_final.count().toLong/3
 }
}
