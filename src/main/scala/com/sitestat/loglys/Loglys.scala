package com.sitestat.loglys

import java.io.PrintStream

import com.sitestat.loglys.api.LoglysApi
import com.sitestat.loglys.data._
import com.sitestat.loglys.util.Utils
import com.sitestat.loglys.util.Utils.TimeClassification

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object Loglys {

  private[loglys] var exitFn: () => Unit = () => System.exit(1)
  private[loglys] var printStream: PrintStream = System.err
  private[loglys] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[loglys] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      _                 _
     / /  __     _     / / _  _   ____
    / /_ / _ \ / _ \  / /  \\/ / /_/__
   /_ _/ \___/ \   / /_/_/  / /  __/_/ version %s
               / _ \       /_/
               \ _ /

                        """.format(LOGLYS_VERSION))
    printStream.println("Type --help for more information.")
    exitFn()
  }
  private[loglys] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn()
  }

  def main(args : Array[String]): Unit = {
    val appArgs = new LoglysArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }

    val sparkConf = new SparkConf()
      .setAppName("Loglys")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val baseUrl = "www.testify.com"
    val artFile = appArgs.artReadFile
    val clickFile = appArgs.clickLogFile

    import sqlContext.implicits._


    // Drop the header first
    val artRec = sc.textFile(artFile, 2).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    // It was like below earlier, but section column can have values like this : "dagbladet/pluss/kultur/tv og medier"
    // This causes problems in parsing based in whitespace. To mitigate this we take another approach where we treat
    // first 7 columns sane, then take the last column as timestamp and merge all the columns in between as the value
    // of 'section' column
    // This also ignores the fact that there was a minTimestamp column earlier, but since now it is no longer the case
    // only the last column is treated as timestamp column
    /* val artDf = artRec.map(_.split("\\s+")).map(
      r => ArticleRead(r(0).trim.toInt, r(1), r(2).trim.toDouble, r(3).trim.toInt, r(4).trim.toInt, r(5).trim.toInt,
        r(6).trim.toInt, r(7).trim.toInt, r(8), r(10).trim.toLong)).toDF.cache
    */
    val artDf = artRec.map(_.split("\\s+")).map(
      r => {
        val sz = r.length
        val section = {
          var i = 8
          var value: String = ""
          while(i<sz-1) {
            value += r(i)
            i += 1
          }
          value
        }
        ArticleRead(r(0).trim.toInt, r(1), r(2).trim.toDouble, r(3).trim.toInt, r(4).trim.toInt, r(5).trim.toInt,
          r(6).trim.toInt, r(7).trim.toInt, section, r(sz-1).trim.toLong)
      }).toDF.cache

    // Drop the header first
    val clickRec = sc.textFile(clickFile, 2).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val clickDf = clickRec.map(_.split("\\s+")).map(
      r => ClickLog(r(0).trim.toInt, r(1), r(2).trim.toLong, r(3))).toDF

    artDf.registerTempTable(LoglysApi.ARTICLEREADTABLE)
    clickDf.registerTempTable(LoglysApi.CLICKLOGTABLE)

    //get enhanced clickLog
    val qj = LoglysApi.getJoinQueryForEnhancedClickLog()
    val rows = sqlContext.sql(qj)

    val enhancedClickDf  = rows.map(r => {
      val timeStamp = r.getLong(r.length-1)
      val (year, month, day, hour) = Utils.yearMonthDay(timeStamp)
      val tRef = r.get(3).toString
      var (vDir, vInt, vFb, vExt) = (0,0,0,0)
      if (tRef.contains(baseUrl)) vInt = 1 else if (tRef == ".") vDir = 1 else if (tRef.contains("facebook")) vFb = 1 else vExt = 1
      EnhancedClickLog(r.getInt(0), r.get(1).toString, r.getLong(2), tRef, timeStamp, year, month, day, Utils.TimeClassification.hourClassifier(hour).toString,
        vDir, vInt, vFb, vExt)
    } ).toDF.cache


    sqlContext.dropTempTable(LoglysApi.CLICKLOGTABLE)
    enhancedClickDf.registerTempTable(LoglysApi.CLICKLOGTABLE)
    
    // cache the tables
    // https://issues.apache.org/jira/browse/SPARK-10030
    // fixed by https://issues.apache.org/jira/browse/SPARK-10422
    // sqlContext.cacheTable(LoglysApi.ARTICLEREADTABLE)
    // sqlContext.cacheTable(LoglysApi.CLICKLOGTABLE)
    // 
    
    /******************************************
     *
     *  PLEASE USE ONLY ONE QUERY AT A TIME TO GET FASTER RESULT. BETTER TO COMMENT OUT OTHER QUERYS 
     *  AND DISPLAY ONLY ONE AT A TIME. 
     * 
     *******************************************/

    // 1. Get traffic by date [day, week, month, year] using timestamp
    println("Get data by date [day = 10, month = aug, year = 2014] - [day = 20, month = aug, year = 2014]")
    
    // start date is 10th august
    val startCal = Utils.getCal(2015, 7, 10)
    
    // end date is 20th august
    val endCal = Utils.getCal(2015, 7, 20)
    val startTime = Utils.getEpochTime(startCal)
    val endTime = Utils.getEpochTime(endCal)

    val query = LoglysApi.getQueryWithTimestampFilter(startTime.toString, endTime.toString)
    println(s"Executing query: $query")

    val results = sqlContext.sql(query).collect
    results.foreach(println)
    

    //2. Get traffic by sources [traffic by facebook, traffic by external, traffic by internal, traffic by direcotry]
    // Get traffic by facebook
    val query2 = LoglysApi.getQueryWithDataSourcesFilter("vFb")
    println(s"Executing query: $query2")
    val results2 = sqlContext.sql(query2).collect
    results2.foreach(println)

    //3. Select distinct external data sources
    val query3 = LoglysApi.getDistinctQuery("tRef", LoglysApi.CLICKLOGTABLE)
    println(s"Executing query: $query3")
    val results3 = sqlContext.sql(query3).collect
    results3.foreach(println)

    //3. Select distinct section
    val query3a = LoglysApi.getDistinctQuery("section", LoglysApi.ARTICLEREADTABLE)
    println(s"Executing query: $query3a")
    val results3a = sqlContext.sql(query3a).collect
    results3a.foreach(println)

    //4. Select sum(tViews) for morning, afternoon, evening, night for aug 13th 2014
    val startPCal = Utils.getCal(2014, 7, 13)
    val outPut = new Array[Long](TimeClassification.values.size)
    var i=0
    var startPoint = Utils.getEpochTime(startPCal)
    for (classification <- TimeClassification.values) {
      val (query, newStartPoint) = LoglysApi.getMorningNightQuery(startPoint, "tviews", classification)
      println(s"Executing query: $query")
      startPoint = newStartPoint
      outPut(i) = sqlContext.sql(query).collect.head.getLong(0)
      i += 1
    }

    println(TimeClassification.values.zip(outPut).mkString("[",",","]"))

    //5. All articles count,  read on 15th aug 2014
    val startACount = Utils.getCal(2014, 7, 15)
    val endACount = Utils.getCal(2014, 7, 20)
    val startATime = Utils.getEpochTime(startACount).toString
    val endATime = Utils.getEpochTime(endACount).toString
    val query4 = LoglysApi.getAllArticleQuery(startATime, endATime)
    println(s"Executing query $query4")
    val results4 = sqlContext.sql(query4).collect
    results4.foreach(println)

    //6. Get records with sources filter
    val query5 = LoglysApi.getQueryWithRefFilter("www.vl.no")
    println(s"Executing query $query5")
    val results5 = sqlContext.sql(query5).collect
    results5.foreach(println)

    //7. Find all articles that are not live yet
    val query6 = LoglysApi.getQueryWithSectionFilter("vl/404")
    println(s"Executing query $query6")
    val results6 = sqlContext.sql(query6).collect
    results6.foreach(println)

    //8. Most Important query!
    val query7 = LoglysApi.getUserQuery
    println(s"Executing query $query7")
    val results7 = sqlContext.sql(query7).collect
    results7.foreach(println)

    val query8 = LoglysApi.getViewsByYear(2014)
    println(s"Executing query $query8")
    val results8 = sqlContext.sql(query8).collect
    results8.foreach(println)

    val query9 = LoglysApi.getCountArticleBySource()
    println(s"Executing query $query9")
    val results9 = sqlContext.sql(query9).collect
    results9.foreach(println)

  }
}
