package com.nmit.spark.ipltosswinstats

import org.apache.spark.sql.SparkSession


object ipltosswinstats {

  def main(args: Array[String]) {

    val pathToDB = "/home/"
    val sparkSession = SparkSession.builder().appName("My SQL Session").getOrCreate()
    import sparkSession.implicits._
  

    val matchDF = sparkSession.read.format("csv").
      option("sep", ",").
      option("inferSchema", "true").
      option("header", "true").
      load(pathToDB + "/Match.csv")
    matchDF.createOrReplaceTempView("matchStats")

    val N = sparkSession.sql("SELECT COUNT(*) FROM matchstats")
      .first()(0)
      .asInstanceOf[Long]

    val tossNMatchwinnersDF = sparkSession.sql("SELECT * FROM matchstats WHERE Toss_Winner_Id = Match_Winner_Id")

    tossNMatchwinnersDF.createOrReplaceTempView("tossNMatchwinners")

    val M = sparkSession.sql("SELECT COUNT(*) FROM tossNMatchwinners")
      .first()(0)
      .asInstanceOf[Long]

    println("Percentage of times Toss Winners have won the match = " + (M*100.0)/N + "%")
  }
}
