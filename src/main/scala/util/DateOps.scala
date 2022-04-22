package util

import java.text.SimpleDateFormat
import java.util.Date

object DateOps {
  implicit class DateEnhancer(val date: Date) extends AnyVal {
    // Due to the Date.getYear() method flagged as deprecated
    def asYear: String = new SimpleDateFormat("yyyy").format(date)
  }
}