/**
  * Created by hongong on 9/12/16.
  */

class IPAddressValidation {
  private[this] val IP6_PATTERN = "([a-f0-9]{1,4}:){7}\\b[0-9a-f]{1,4}\\b"
  private[this] val IP4_PATTERN =
    "\\b(((2[0-5][0-5])|(1[0-9][0-9])|(\\b[1-9][0-9]\\b)|(\\b\\d\\b))\\.){3}((2[0-5][0-5])|(1[0-9][0-9])|(\\b[1-9][0-9]\\b)|(\\b\\d\\b))\\b"

  def ipValidation(address: String): Boolean = {
    if (address.matches(IP6_PATTERN))
      true
    else if (address.matches(IP4_PATTERN))
      true
    else
      false
  }
}