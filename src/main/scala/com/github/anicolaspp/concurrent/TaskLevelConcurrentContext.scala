package com.github.anicolaspp.concurrent

import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.Executors

import scala.concurrent.duration.Duration._

/**
  * This is the default ConcurrentContext.
  *
  * We use a CachedThreadPool so we can spawn new threads if needed, but reused them as they become available.
  */
private[concurrent] object TaskLevelConcurrentContext extends ConcurrentContext {

  /**
    * We are using CachedThreadPool which is the same as the default used by Spark to run multiple tasks within an Executor.
    */
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  override def async[A](task: => A): Future[A] = Future(task)

  override def awaitSliding[A](it: Iterator[Future[A]], batchSize: Int = 20): Iterator[A] = {
    val slidingIterator = it.sliding(batchSize - 1).withPartial(true)

    val (head, tail) = slidingIterator.span(_ => slidingIterator.hasNext)

    head.map(batchOfFuture => Await.result(batchOfFuture.head, Inf)) ++
      tail.flatMap(batchOfFuture => Await.result(Future.sequence(batchOfFuture), Inf))
  }
}
