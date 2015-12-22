import java.util.{Calendar, TimeZone}
import java.util.Locale

 val hourClassifier = new scala.collection.mutable.HashMap[Int, String]()
    (for (i <- 1 until 12)  hourClassifier += (i -> "Morning"))
    (for (i <- 12 until 16) hourClassifier += (i -> "AfterNoon"))
    (for (i <- 16 until 20) hourClassifier += (i -> "Evening"))
    (for (i <- 20 until 25) hourClassifier += (i -> "Night"))
    hourClassifier += (0 -> "Night")

 case class ArticleRead(id: Int, url: String, avage: Double, tviews: Int, vDir: Int, vInt: Int, vFb: Int, vExt: Int, section: String, timeStamp: Long)

 case class ClickLog(id: Int, tSession: String, tAge: Long, tRef: String)

case class EnhancedArticleRead(id: Int, url: String, avage: Double, tviews: Int,
  vDir: Int, vInt: Int, vFb: Int, vExt: Int, section: String, timeStamp: Long, subDomain: String)

//case class EnhancedClickLog(id: Int, tSession: String, tAge: Long, tRef: String, timeStamp: Long, year: Int, month: Int, day: Int, classifier: String)
case class EnhancedClickLog(id: Int, tSession: String, tAge: Long, tRef: String, timeStamp: Long, year: Int, month: Int,
  day: Int, classifier: String, iDir: Byte, iInt: Byte, iFb: Byte, iExt: Byte)

 // Drop the header first
    val artRec = sc.textFile("/Users/sureshkumarmukhiya/Downloads/basis/dagbladet/artread", 4).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

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
    val clickRec = sc.textFile("/Users/sureshkumarmukhiya/Downloads/basis/dagbladet/clicklog", 4).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val clickDf = clickRec.map(_.split("\\s+")).map(
      r => ClickLog(r(0).trim.toInt, r(1), r(2).trim.toLong, r(3))).toDF

    artDf.registerTempTable("articleRead")
    clickDf.registerTempTable("clickLog")

  
    //get enhanced clickLog
    val qj = "select clickLog.id, clickLog.tSession,clickLog.tAge,clickLog.tRef, (articleRead.timeStamp + clickLog.tAge) as timeStamp from articleRead, clickLog where articleRead.id = clickLog.id"
   
    // LoglysApi.getJoinQueryForEnhancedClickLog()
    val rows = sqlContext.sql(qj)
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
   
  // val baseUrl = "www.testify.com"
   
    val enhancedClickDf  = rows.map(r => {
      val timeStamp = r.getLong(r.length-1)
    val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US)   
    calendar.setTimeInMillis(timeStamp*1000)

    val (year, month, day, hour) = (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY))
      //val (year, month, day, hour) = (0,0,0,0)//f(timeStamp)
      val clas = if (hour < 12) "Morning" else if (hour < 16) "AfterNoon" else if (hour < 20) "Evening" else "Night"//hourClassifier(hour)
       val tRef = r.get(3).toString
      var (vDir, vInt, vFb, vExt) = (0.toByte,0.toByte,0.toByte,0.toByte)
      if (tRef.contains("www.dagsavisen.no")) vInt = 1.toByte else if (tRef == ".") vDir = 1.toByte else if (tRef.contains("facebook")) vFb = 1.toByte else vExt = 1.toByte
      
      EnhancedClickLog(r.getInt(0), r.get(1).toString, r.getLong(2), tRef, timeStamp, year, month, day, clas, vDir, vInt, vFb, vExt)
    } ).toDF().cache

    sqlContext.dropTempTable("clickLog")
    enhancedClickDf.registerTempTable("clickLog")