package com.pg.bigdata.octopufs.fs

/**
 * Represents File System operation result on a path
 * @param path
 * @param success
 */
case class FsOperationResult(path: String, success: Boolean)