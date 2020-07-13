package com.github.anicolaspp.concurrent

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

/**
  * This is the default ConcurrentContext.
  *
  * We use a CachedThreadPool so we can spawn new threads if needed, but reused them as they become available.
  */
private[concurrent] class BoundedConcurrentContext(size: Int = 24) extends ConcurrentContext {

  /**
    * We are using CachedThreadPool which is the same as the default used by Spark to run multiple tasks within an Executor.
    */
  override def ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(size))
}

object BoundedConcurrentContext {
  def apply(size: Int = 24): BoundedConcurrentContext = new BoundedConcurrentContext(size)
}


