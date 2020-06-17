package com.pg.bigdata.octopufs.fs

case class FsElement(path: String, isDirectory: Boolean, byteSize: Long) {
 /* def toRelativePath() = {
    FSElement(getRelativePath(path),isDirectory,byteSize)
}*/
}
