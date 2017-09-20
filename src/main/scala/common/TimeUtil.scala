package common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TimeUtil {

  private val YEAR = "yyyy"
  private val MONTH = "MM"
  private val DAY = "dd"
  private val TIME = "HHmmss"

  var date: Date = Calendar.getInstance().getTime

  def getCurrentYear  = new SimpleDateFormat(YEAR).format(date)
  def getCurrentMonth = new SimpleDateFormat(MONTH).format(date)
  def getCurrentDay   = new SimpleDateFormat(DAY).format(date)
  def getCurrentTime  = new SimpleDateFormat(TIME).format(date)
  def resetDate       = { date = Calendar.getInstance().getTime }
}
