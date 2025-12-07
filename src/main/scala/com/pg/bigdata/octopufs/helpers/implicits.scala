package com.pg.bigdata.octopufs.helpers

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
 * implicits objects contains variables which can be modified to change parallelism of multithreaded operations. In order to change maximum parallelism,
 * overwrite executionContext: ExecutionContextExecutor variable. To change default timeout, change fsOperationTimeoutMinutes
 */
object implicits {
  var exec: ExecutorService = Executors.newFixedThreadPool(1000)
  implicit var executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(exec)
  val fsOperationTimeoutMinutes: Int = 10
}
