
/**
  * Created by hongong on 9/12/16.
  */

case class Extras(mobile: Option[String],
                  adunit: Option[String],
                  userAgent: Option[String],
                  guid: Option[String],
                  device: Option[String],
                  clickthrough: Option[Boolean],
                  others: Option[String])

object Extras {
  def minimal(): Extras =
    Extras(Some(""), Some(""), Some(""), Some(""), Some(""), Some(false), Some(""))

  def invalid(): Extras =
    Extras(Some(""), Some(""), Some(""), Some(""), Some(""), Some(false), Some(""))
}