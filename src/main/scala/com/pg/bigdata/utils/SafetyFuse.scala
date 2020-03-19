package com.pg.bigdata.utils

import org.apache.hadoop.fs.{FileSystem, Path}

class SafetyFuse(folderUri: String, runId: String = SafetyFuse.defaultRunId)(implicit fs: FileSystem) {
  val fusePath: String = folderUri + {
    if (!folderUri.endsWith("/")) "/" else ""
  } + runId + "_open_transaction"

  def isInProgress(): Boolean = fs.exists(new Path(fusePath))

  def startTransaction(): Unit = {
    if (!fs.createNewFile(new Path(fusePath))) throw new Exception("Could not create transaction marker "+fusePath)
    println("Fuse " + fusePath + " deleted")
  }

  def endTransaction(): Unit = {
    if (!fs.delete(new Path(fusePath), false))
      throw new Exception("Could not delete transaction marker "+fusePath+". Delete it manually, but do NOT rerun your job unless you REALLY know what you're doing")
    println("Fuse " + fusePath + " deleted")
  }

  override def toString: String = "Fuse path: " + fusePath

}

object SafetyFuse {
  var defaultRunId = "defaultRunId"
}