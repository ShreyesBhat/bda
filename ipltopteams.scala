package com.nmit.spark.ipltopteams

import org.apache.spark.sql.SparkSession

/**
  *  Problem statement:
  *  1. If the user runs the program with argument "1", print the schemas of all the tables stored in each csv file
  *     of the indian-premier-league-csv-dataset.
  *  2. If the user runs the program with argument "2", print the names of the top three winningest teams in terms of the
  *     number of matches won. The code should use dataframes and dataframe operations.
  *
 */

object iplstats {

 def main(args: Array[String]) {

   val pathToDB = "/home/subhrajit/sparkProjects/data/indian-premier-league-csv-dataset"
   val sparkSession = SparkSession.builder().appName("My SQL Session").getOrCreate()
   import sparkSession.implicits._
  
   def print_all_schemas() {
   
     val matchDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Match.csv")

     println("Schema: Match Data")
     matchDF.printSchema()
   
     val playerDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Player.csv")

     println("Schema: Player Data")
     playerDF.printSchema()

     val player_matchDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Player_Match.csv")

     println("Schema: Player_Match Data")
     player_matchDF.printSchema()

     val season_matchDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Season.csv")

     println("Schema: Season Match Data")
     season_matchDF.printSchema()

     val teamDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Team.csv")

     println("Schema: Team Data")
     teamDF.printSchema()

     val ballByBallDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Ball_by_Ball.csv")

     println("Schema: Ball By Ball Data")
     ballByBallDF.printSchema()
   }

   // Print the names of the top three teams in terms of winning the most games.

   def print_top_three() {

     // Step 1. From match data, find the number of games won by each team_id.
     // Select required columns from match data DF.
     // |-- Match_Winner_Id: integer (nullable = true)

     val matchDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Match.csv")

     val matchWinners = matchDF.select("Match_Winner_Id")
     val matchesWon = matchWinners.groupBy("Match_Winner_Id").count()

     // Step 2. Join with Team data to find the names of the team for the team_id's.
     //         Sort them in descending order of games won.
     //         Print the names of the top three teams.

     val teamDF = sparkSession.read.format("csv").
       option("sep", ",").
       option("inferSchema", "true").
       option("header", "true").
       load(pathToDB + "/Team.csv")

     val matchesWonSortedWithTeamNames = matchesWon.
       join(teamDF, matchesWon.col("Match_Winner_ID").equalTo(teamDF("Team_Id"))).
       sort($"count".desc)

     matchesWonSortedWithTeamNames.show(3)
   }

   if (args.length != 1) {
      println()
      println("Please specify whether you want to print schemas of tables in DB by specifying argument as 1.")
      println("Or if you want to find the top three teams, specify argument as 2.")
      System.exit(1)
   }

   if ((args.length == 1) && (args(0) == "1"))
     print_all_schemas()
   if ((args.length == 1) && (args(0) == "2"))
     print_top_three()

 }


}
