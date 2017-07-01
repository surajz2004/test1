package com.kogentix.dl_poc.source_adaptor

import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectListing
import org.apache.spark.{SparkConf, SparkContext}
//import com.kogentix.amp.sa.file.FileSourceAdapter
//import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{ DataFrame, SQLContext }
//import org.json4s.NoTypeHints
//import org.json4s.jackson.Json
//import org.json4s.jackson.Serialization
//import org.json4s.jackson.Serialization._
import org.slf4j.LoggerFactory
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.collection.JavaConversions._
import scala.util.Try

case class TableName(table_name: String)
case class SchemaName(schema_name: String)

class S3SourceAdapter(val key: String, val secret: String, config: Map[String, String] = Map()) {
  val s3Credentials = new BasicAWSCredentials(key, secret)
  val s3Client = new AmazonS3Client(s3Credentials)


//  def toJson(any: AnyRef): String = {
//    implicit val formats = Serialization.formats(NoTypeHints)
//    val json = write(any)
//    json
//  }

  /**
    *
    * @return in this format - {\"table_name\":\"t1c\"}","{\"table_name\":\"t1p1\"}","{\"table_name\":\"t1p2\"}
    *
    */
  def getBuckets(): Seq[SchemaName] = {
    val buckets = s3Client.listBuckets()

    val bucketNames = for {
      b <- buckets
    } yield SchemaName(b.getName)

    bucketNames
  }

  val acceptFileWithExtension = Set("txt", "csv", "tsv")

  def filter(fileName: String): Boolean = {
    val lower = fileName.toLowerCase()
    if (lower.contains(".")) {
      val ext = lower.substring(lower.lastIndexOf(".")).tail
      acceptFileWithExtension.contains(ext)
    } else {
      false
    }
  }

  /**
    *
    * @return in this format - {\"table_name\":\"t1c\"}","{\"table_name\":\"t1p1\"}","{\"table_name\":\"t1p2\"}
    *
    */
  def getFiles(bucketName: String): Seq[TableName] = {
    require(bucketName != null, "bucketName is required")

    val objectList: ObjectListing = s3Client.listObjects(bucketName)

    val fileNames = for {
      o <- objectList.getObjectSummaries

      if !o.getKey.endsWith("/")
      if filter(o.getKey)
    } yield TableName(o.getKey)

    fileNames
  }
}


object S3SourceAdapter {

  val logger = LoggerFactory.getLogger(S3SourceAdapter.getClass)

  def updateSqlContext(sqlContext: SQLContext, key: String, secret: String): SQLContext = {
    val sc = sqlContext.sparkContext
    logger.info(s"using all $key $secret")
    //    sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //    sc.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", key)
    //    sc.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secret)

    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", key)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)
    sqlContext
  }

 /* def getDataframeCsv(sqlContext: SQLContext, butcketName: String, fileName: String, key: String, secret: String,
                      optionMap: Map[String, String]): DataFrame = {

    updateSqlContext(sqlContext, key, secret)

    val location = s"s3n://${butcketName}/${fileName}"

    // if csv file
    logger.info(s"Connecting 2 to location using $location $optionMap")
    val inferedOptions = FileSourceAdapter.buildOption(sqlContext, location, optionMap)

    logger.info(s"inferedOptions $inferedOptions")
    FileSourceAdapter.createDfFromFile(sqlContext, location, inferedOptions)
  }*/

  /*def getDataframeParquet(sqlContext: SQLContext, butcketName: String, fileName: String, key: String, secret: String,
                          optionMap: Map[String, String]): DataFrame = {

    updateSqlContext(sqlContext, key, secret)

    val location = s"s3n://${butcketName}/${fileName}"

    val df = sqlContext.read.options(optionMap).parquet(location)
    df
  }*/
  def getDataframeAvro(sqlContext: SQLContext, bucketName: String, fileName: String, key: String, secret: String): DataFrame = {
    updateSqlContext(sqlContext, key, secret)

    val location = s"s3n://${bucketName}/${fileName }"
    //val location = "file:///home/shresu02/vamshi/"

    //val schema = new Schema.Parser().parse(new File("user.avsc"))
    //val spark = SparkSession.builder().master("local").getOrCreate()
    val df = sqlContext.read.format("com.databricks.spark.avro").load(location)//avro(location)
    //df.filter("doctor > 5").write.avro("/tmp/output")
    /*spark
	    .read
	    .format("com.databricks.spark.avro")
	    .option("avroSchema", schema.toString)
	    .load("src/test/resources/episodes.avro").show()*/
    //df.filter("doctor > 5").write.format("avro").w("/tmp/output")
    df.write.format("com.databricks.spark.avro").save("/tmp/output")
    val count = df.count()
    df.show()
    df
  }


  def main(args: Array[String]): Unit = {
    val key = "AKIAJOPWSR2TQAR4Y2FQ"
    val secret = "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"
    // s3n://amptestdir/connection_info.txt
    val butcketName = "amptestdir"
    val fileName = "twitter.avro"
    //val spark = SparkSession.builder().master("local").getOrCreate();

    val conf = new SparkConf().setAppName("test").setMaster("local");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    getDataframeAvro(sqlContext, butcketName, fileName, key, secret);
  }


}


