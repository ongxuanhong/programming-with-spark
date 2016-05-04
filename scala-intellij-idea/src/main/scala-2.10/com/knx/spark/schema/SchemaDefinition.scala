package com.knx.spark.schema

/**
  * Created by hongong on 5/3/16.
  */

import org.apache.spark.sql.types.{DataType, StructField}

import scala.collection.mutable.ListBuffer

trait SchemaDefinition extends Serializable {
  private[this] val builder = ListBuffer.empty[StructField]

  protected def structField(name: String, dataType: DataType): StructField = {
    val field = StructField(name, dataType, nullable = true)
    builder += field
    field
  }

  def fields: Seq[StructField] = builder.toSeq
}
