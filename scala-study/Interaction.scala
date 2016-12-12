import org.apache.spark.sql.functions.udf

/**
  * Created by hongong on 9/12/16.
  */

case class Interaction(kinesisId: Option[String],
                       widgetId: Option[String],
                       url: Option[String],
                       referer: Option[String],
                       trigger: Option[String],
                       `object`: Option[String],
                       response: Option[String],
                       extra: Option[String],
                       agent: Option[String],
                       time: Option[Long],
                       uid: Option[String],
                       extras: Option[Extras],
                       remoteAddr: Option[String],
                       os: Option[String],
                       device: Option[String],
                       section: Option[String],
                       browser: Option[String],
                       interactionTime: Option[Int]) {
  def setSection(section: String): Interaction = copy(section = Some(section))

  def setOs(os: String): Interaction = copy(os = Some(os))

  def setBrowser(browser: String): Interaction = copy(browser = Some(browser))

  def setUserAgent(os: String, device: String, browser: String): Interaction = copy(os = Some(os), device = Some(device), browser = Some(browser))

  def setTime(time: Long): Interaction = copy(time = Some(time))

  def setInteractionTime(interactionTime: Int): Interaction = copy(interactionTime = Some(interactionTime))

}

object Interaction {
  def minimal(id: String): Interaction =
    Interaction(Some(id), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)

  def invalid(): Interaction =
    Interaction(Some("-1"), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)

  val arrayOfStringContains: ((Array[String], String) => Boolean) = (arrayOfString: Array[String], stringToBeUsed: String) => {
    arrayOfString.contains(stringToBeUsed)
  }

  val arrayOfStringContainsSqlFunc = udf(arrayOfStringContains)

  val getArrayOfString: (List[String] => Array[String]) = (arrayOfString: List[String]) => {
    arrayOfString.toArray
  }

  val getArrayOfStringSqlFunc = udf(getArrayOfString)
}