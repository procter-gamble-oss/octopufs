package com.pg.bigdata.octopufs.helpers

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

object implicits {
  var exec = Executors.newFixedThreadPool(1000)
  implicit var pool = ExecutionContext.fromExecutor(exec)
  val fsOperationTimeoutMinutes: Int = 10
}
