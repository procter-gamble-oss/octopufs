package com.pg.bigdata.octopufs.fs

/**
 * Represents element of a file system
 * @param path
 * @param isDirectory
 * @param byteSize
 */
case class FsElement(path: String, isDirectory: Boolean, byteSize: Long) {

}
