package com.sitestat.loglys.api

import com.sitestat.loglys.data.{ArticleRead, ClickLog}
import com.sitestat.loglys.util.Utils
import com.sitestat.loglys.util.Utils.TimeClassification._
/**
 * Created by Suresh Kumar Mukhiya <itsmeskm99@gmail.com> on 26/09/15.
 */
object LoglysApi {

  val ARTICLEREADTABLE = "articleRead"
  val CLICKLOGTABLE = "clickLog"
  val JOINCOL = "id"

  val clickDfFields = Utils.getFields[ClickLog](LoglysApi.JOINCOL).map(x => s"$CLICKLOGTABLE.$x").mkString(",")
  val artDfFields = Utils.getFields[ArticleRead](LoglysApi.JOINCOL).map(x => s"$ARTICLEREADTABLE.$x").mkString(",")

  val enhancedClickDfFields = List("tSession", "tAge", "tRef", "year", "month", "day", "classifier").map(x => s"$CLICKLOGTABLE.$x").mkString(",")

  def getJoinQueryForEnhancedClickLog(): String = {
    s"select $CLICKLOGTABLE.id, $clickDfFields, ($ARTICLEREADTABLE.timeStamp + $CLICKLOGTABLE.tAge) as timeStamp from $ARTICLEREADTABLE, $CLICKLOGTABLE " +
      s"where $ARTICLEREADTABLE.id = $CLICKLOGTABLE.id"
  }

  def getQueryWithTimestampFilter(startTime: String, endTime: String): String = {
    s"select $ARTICLEREADTABLE.id, $artDfFields, $clickDfFields from $ARTICLEREADTABLE , $CLICKLOGTABLE " +
      s"where $ARTICLEREADTABLE.id = $CLICKLOGTABLE.id and $ARTICLEREADTABLE.timeStamp >= $startTime and $ARTICLEREADTABLE.timeStamp < $endTime"
  }

  def getQueryWithDataSourcesFilter(dataSource: String): String = {
    s"select $ARTICLEREADTABLE.id, $artDfFields, $clickDfFields from $ARTICLEREADTABLE , $CLICKLOGTABLE " +
      s"where $ARTICLEREADTABLE.id = $CLICKLOGTABLE.id and $ARTICLEREADTABLE.$dataSource > 0"
  }

  def getQueryWithRefFilter(ref: String): String = {
    s"select $ARTICLEREADTABLE.id, $artDfFields, $clickDfFields from $ARTICLEREADTABLE , $CLICKLOGTABLE " +
      s"where $ARTICLEREADTABLE.id = $CLICKLOGTABLE.id and $CLICKLOGTABLE.tRef = \'$ref\' "
  }

  def getQueryWithSectionFilter(section: String): String = {
    s"select $ARTICLEREADTABLE.id, $artDfFields, $clickDfFields from $ARTICLEREADTABLE , $CLICKLOGTABLE " +
      s"where $ARTICLEREADTABLE.id = $CLICKLOGTABLE.id and $ARTICLEREADTABLE.section = \'$section\' "
  }

  def getDistinctQuery(colName: String, tableName: String): String = {
    s"select distinct($colName) from $tableName"
  }

  def getMorningNightQuery(startPoint: Long, sumCol: String, classification: TimeClassification): (String, Long) = {
    val (startTime, endTime) = (startPoint, (startPoint + duration(classification) + 1))
    (s"select sum($sumCol) from $ARTICLEREADTABLE where timeStamp >= $startTime and timeStamp < $endTime", endTime)
  }


  def getAllArticleQuery(startTime: String, endTime: String): String = {
    s"select distinct(id), sum(tviews) from $ARTICLEREADTABLE " +
      s"where timeStamp >= $startTime and timeStamp < $endTime group by id"
  }

  def getUserQuery(): String = {
    s"select sum(1) as total_views, sum(iFb) as fb, sum(iDir) as direct, sum(iInt) as internal, sum(iExt) as external from $CLICKLOGTABLE " +
      s"group by tSession"
  }

  def getViewsByYear(year: Int): String = {
    s"select count(distinct tSession) as views, count(distinct id) as articleIds, month from $CLICKLOGTABLE where year=$year group by month"
  }

  def getCountArticleBySource(): String = {
    s"select tRef as Source, count(distinct id) as articles, count(distinct tSession) as views from $CLICKLOGTABLE group by tRef"
  }


}
