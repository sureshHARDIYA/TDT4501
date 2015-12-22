package com.sitestat.loglys.util

import java.util.{Locale, Calendar, TimeZone}

import scala.reflect.runtime.universe._
import scala.collection.immutable.HashMap

/**
 * Created by Suresh Kumar Mukhiya <itsmeskm99@gmail.com> on 26/09/15.
 */
object Utils {

  object TimeClassification extends Enumeration {
    type TimeClassification = Value
    val Morning, AfterNoon, Evening, Night = Value

    val duration = HashMap[TimeClassification, Int](
      (Morning->43199),
      (AfterNoon->14399),
      (Evening->14399),
      (Night->14399))

    val hourClassifier = new scala.collection.mutable.HashMap[Int, TimeClassification]()
    (for (i <- 1 until 12)  hourClassifier += (i -> Morning))
    (for (i <- 12 until 16) hourClassifier += (i -> AfterNoon))
    (for (i <- 16 until 20) hourClassifier += (i -> Evening))
    (for (i <- 20 until 25) hourClassifier += (i -> Night))
    hourClassifier += (0 -> Night)
  }

  def getCalSeconds(year: Int, month: Int, day: Int, hour: Int = 0, minutes: Int = 0, seconds: Int = 0): Calendar = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US)
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month)
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, minutes)
    calendar.set(Calendar.SECOND, seconds)
    calendar
  }

  def getEpochTime(cal: Calendar): Long = cal.getTimeInMillis/1000

  def getCal(year: Int, month: Int, day: Int): Calendar = {
    getCalSeconds(year, month, day)
  }

  def yearMonthDay(timeStamp: Long): (Int, Int, Int, Int) = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.US)
    calendar.setTimeInMillis(timeStamp*1000)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY))
  }

  def getFields[T: TypeTag](idx: String): Array[String] = {
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.map(_.name.toString).filter(_ != idx).toArray.reverse

  }

}
