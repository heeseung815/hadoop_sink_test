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

TimeUtil.getCurrentYear
TimeUtil.getCurrentMonth
//
//
//val cal = Calendar.getInstance()
//val year = cal.get(Calendar.YEAR)
//val month = cal.get(Calendar.MONTH)
//val day = cal.get(Calendar.DATE)
////val time = cal.get(Calendar.)
//
//val date = Calendar.getInstance().getTime
//
//val fmt = new SimpleDateFormat("yyyy-MM-dd-HHmmss")
//fmt.format(date)
//val fmt2 = new SimpleDateFormat("HHmmss")
//fmt2.format(date)
//val fmt3 = new SimpleDateFormat("dd")
//fmt3.format(date)
//val fmt4 = new SimpleDateFormat("MM")
//fmt4.format(date)
//val fmt5 = new SimpleDateFormat("yyyy")
//fmt5.format(date)
//
//val temp = fmt.format(cal.getTime).split("-")
//
//
//temp.foreach(println(_))

