package com.sitestat.loglys.data

import com.sitestat.loglys.util.Utils.TimeClassification.TimeClassification

/**
 * Created by Suresh Kumar Mukhiya <itsmeskm99@gmail.com> on 25/09/15.
 */
case class ArticleRead(id: Int, url: String, avage: Double, tviews: Int, vDir: Int, vInt: Int, vFb: Int, vExt: Int, section: String, timeStamp: Long)

case class ClickLog(id: Int, tSession: String, tAge: Long, tRef: String)

case class EnhancedArticleRead(id: Int, url: String, avage: Double, tviews: Int,
  vDir: Int, vInt: Int, vFb: Int, vExt: Int, section: String, timeStamp: Long, subDomain: String)

case class EnhancedClickLog(id: Int, tSession: String, tAge: Long, tRef: String, timeStamp: Long, year: Int, month: Int,
  day: Int, classifier: String, iDir: Int, iInt: Int, iFb: Int, iExt: Int)