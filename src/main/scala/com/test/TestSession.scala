package com.test

import org.apache.spark.sql.SparkSession

/**
  * Created by sshrestha on 6/30/17.
  */
object TestSession {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
        .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    System.out.println("done ****** ****");
  }
}
