package com.pg.bigdata.utils

case class DeltaEntry(path: String, message: String){
  override def toString: String = path + " ->" + message
}

object DeltaEntry{
  val MISSING_IN_TARGET = "copy"
  val ONLY_IN_TARGET = "delete"
}