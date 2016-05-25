package com.knx.spark.schema

/**
  * Created by hongong on 5/3/16.
  */

import org.apache.spark.sql.types._
object ImpressionLog {
  self =>
  val widgetId = "widgetId"
  val url = "url"
  val os = "os"
  val device = "device"
  val browser = "browser"
  val referer = "referer"
  val extras = "extras"
  val time = "time"
  val uid = "uid"
  val remoteAddr = "remoteAddr"
  val delayed = "delayed"

  object Schema extends SchemaDefinition {
    val widgetId = structField(self.widgetId, StringType)
    val url = structField(self.url, StringType)
    val referer = structField(self.referer, StringType)
    val time = structField(self.time, LongType)
    val uid = structField(self.uid, StringType)
    val remoteAddr = structField(self.remoteAddr, StringType)
    val delayed = structField(self.delayed, IntegerType)
    val extras = structField(self.extras, StructType(
      StructField("adunit", StringType, true) ::
        StructField("guid", StringType, true) ::
        StructField("others", StringType, true) :: Nil
    ))
    val os = structField(self.os, StructType(
      StructField("family", StringType, true) ::
        StructField("major", StringType, true) ::
        StructField("minor", StringType, true) ::
        StructField("patch", StringType, true) ::
        StructField("patch_minor", StringType, true) :: Nil
    ))
    val browser = structField(self.browser, StructType(
      StructField("family", StringType, true) ::
        StructField("major", StringType, true) ::
        StructField("minor", StringType, true) ::
        StructField("patch", StringType, true) :: Nil))
    val device = structField(self.device, StructType(
      StructField("family", StringType, true) :: Nil))
  }

  val schema: StructType = StructType(Schema.fields)

}