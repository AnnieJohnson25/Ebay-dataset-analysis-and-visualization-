package org.anniejohnson.bidsfilestream
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._;
object bidsfilestreamToES {
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate:Integer, openbid: Float, price: Float, item: String, daystolive: Integer)
def main(args:Array[String])
{
//Old method
//val sparkConf = new SparkConf().setAppName("textstream").setMaster("local[*]")
//val sparkcontext = sparkSession.SparkContext(sparkConf)
// Create the context
val sparkSession = SparkSession.builder.appName("textstream").enableHiveSupport.master("local[*]").config("es.nodes","localhost").config("es.port", "9200").config("es.index.auto.create", "true").getOrCreate();
val sparkcontext = sparkSession.sparkContext;
//val sqlcontext = sparkSession.sqlContext;
sparkcontext.setLogLevel("ERROR")
val ssc = new StreamingContext(sparkcontext, Seconds(10))
//val auctionRDD = sparkcontext.textFile("file:///home/hduser/sparkdata/streaming/");
val auctionRDD = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")
// create an RDD of Auction objects
// change ebay RDD of Auction objects to a DataFrame
import sparkSession.implicits._
// Foreach rdd function is an iterator on the streaming micro batch rdds as like map function

auctionRDD.foreachRDD(rdd => {
  print("Inside FOREACH")
if(!rdd.isEmpty)//making sure that there is data in the source folder which has been made into an rdd
{
//splitting the data based on ~ delimiter and applying the Auction case class on it
val ebay = rdd.map(_.split("~")).map(p => Auction(p(0), p(1).toFloat, p(2).toFloat,p(3),p(4).toInt, p(5).toFloat, p(6).toFloat, p(7), p(8).toInt))

//creating a dataframe on the rdd on which case class has been applied
val ebaydf = ebay.toDF;

//creating a temporary view on which sql operations are carried out
ebaydf.createOrReplaceTempView("ebayview");

//counting the number of records of data present in the dataframe and then finding parameters like max bid, sum of prices from ebayview and storing that data into grpbid rdd 
val auctioncnt = sparkSession.sql("select count(1) from ebayview");
print("executing table query\n")
print("total auctions are\n")
auctioncnt.show(1,false);
print("Executing dataframe\n")
print("total distinct auctions\n")
val count = ebaydf.select("auctionid").distinct.count
System.out.println(count);
print("Executing dataframe\n")
print("Max bid, sum of price of items are\n")
import org.apache.spark.sql.functions._
val grpbid =ebaydf.groupBy("item").agg(max("bid").alias("max_bid"),sum("price").alias("sp")).sort($"sp".desc)
grpbid.show(10,false);

val prop = new java.util.Properties()
prop.put("user", "root")
prop.put("password", "root")


//inserting the ebaydf data into mysql as a table called ebayAuctionData
ebaydf.write.mode("append").jdbc("jdbc:mysql://localhost/custdb", "ebayAuctionData", prop)

println("Data inserted into mysql database..")

ebaydf.saveToEs("ebayauction/ebay")
println("Data inserted into Elastic Search Store..")

}
})


//to continuously check for changes or updates in data in the source folder
print("streaming executing in x seconds\n")
auctionRDD.print()
ssc.start()
ssc.awaitTermination()

}
}
