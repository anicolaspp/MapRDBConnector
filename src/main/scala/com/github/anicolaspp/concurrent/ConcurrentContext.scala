package com.github.anicolaspp.concurrent

import scala.concurrent.Future

/**
  * TaskLevelConcurrentContext is used to control a multithreaded context within a Spark Task.
  */
trait ConcurrentContext {

  /**
    * Wraps a block within a concurrent tasks
    *
    * @param task Block to be executed in concurrently.
    * @tparam A Result type of the passed in block.
    * @return A concurrent task, that is a Future[A].
    */
  def async[A](task: => A): Future[A]

  /**
    * Awaits for multiple concurrent tasks using a sliding windows so we don't have to hold all task results in memory
    * at once.
    *
    * @param it        Iterator of cuncurrent tasks.
    * @param batchSize The number of concurrent tasks we want to wait at a time.
    * @tparam A Result type of each concurrent task.
    * @return An iterator that contains the result of executing each concurrent task.
    */
  def awaitSliding[A](it: Iterator[Future[A]], batchSize: Int = 20): Iterator[A]
}

object ConcurrentContext {
  /**
    * Implicit instance to our TaskLevelConcurrentContext since it is our default one.
    */
  implicit val defaultConcurrentContext: ConcurrentContext = TaskLevelConcurrentContext


  /**
    * Implicit syntax
    */
  object Implicits {

    implicit class ConcurrentIteratorOps[A](it: Iterator[Future[A]]) {
      def awaitSliding(batchSize: Int = 20)(implicit concurrentContext: ConcurrentContext): Iterator[A] =
        concurrentContext.awaitSliding(it, batchSize)
    }

    implicit class AsyncOps[A](task: => A) {
      def async(implicit concurrentContext: ConcurrentContext): Future[A] =
        concurrentContext.async(task)
    }

  }

}