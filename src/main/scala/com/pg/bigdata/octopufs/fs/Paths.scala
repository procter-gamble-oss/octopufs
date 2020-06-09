package com.pg.bigdata.octopufs.fs

case class Paths(sourcePath: String, targetPath: String) {
  override def toString(): String = sourcePath + " -->> " + targetPath
}
