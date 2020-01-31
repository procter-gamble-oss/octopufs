package com.pg.bigdata.utils.fs

case class Paths(sourcePath: String, targetPath: String) {
  override def toString(): String = sourcePath + " -->> " + targetPath
}
