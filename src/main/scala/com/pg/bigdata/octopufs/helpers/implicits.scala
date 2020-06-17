package com.pg.bigdata.octopufs.helpers

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * implicits objects contains variables which can be modified to change parallelism of multithreaded operations. In order to change maximum parallelism,
 * overwrite executionContext variable. To change default timeout, change fsOperationTimeoutMinutes
 */
object implicits {
  var exec = Executors.newFixedThreadPool(1000)
  implicit var executionContext = ExecutionContext.fromExecutor(exec)
  val fsOperationTimeoutMinutes: Int = 10
}
