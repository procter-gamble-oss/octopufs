package com.pg.bigdata.octopufs.fs

/**
 * Represents pair of paths for copy operation
 * @param sourcePath
 * @param targetPath
 */
case class Paths(sourcePath: String, targetPath: String) {
  override def toString(): String = sourcePath + " -->> " + targetPath
}
