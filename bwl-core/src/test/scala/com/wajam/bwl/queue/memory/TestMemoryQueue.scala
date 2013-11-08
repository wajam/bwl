package com.wajam.bwl.queue.memory

import scala.concurrent.Future
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import com.wajam.bwl.queue.{QueueItem, Priority, QueueTask, QueueDefinition}
import com.wajam.bwl.FeederTestHelper._
import com.wajam.nrv.service.Service
import com.wajam.bwl.QueueStatsHelper


@RunWith(classOf[JUnitRunner])
class TestMemoryQueue extends FlatSpec {

  private def task(taskId: Long, priority: Int = 1) = QueueItem.Task("name", token = taskId, priority, taskId, data = taskId)

  trait WithQueue extends MockitoSugar {
    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))
    val definition: QueueDefinition = QueueDefinition("name", (_) => mock[Future[QueueTask.Result]], priorities = priorities)

    val queue = MemoryQueue.create(0, definition, mock[Service])
  }

  import QueueStatsHelper.QueueStatsVerifier

  "Queue" should "enqueue and produce tasks" in new WithQueue {
    // Verification before enqueue
    queue.feeder.take(20).flatten.toList should be(Nil)
    queue.stats.verifyEqualsTo(totalTasks = 0, pendingTasks = Nil)

    // Enqueue
    val t1 = queue.enqueue(task(taskId = 1L, priority = 1))
    val t2 = queue.enqueue(task(taskId = 2L, priority = 1))
    val t3 = queue.enqueue(task(taskId = 3L, priority = 1))

    // Verification after enqueue
    queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
    queue.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))
    queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = List(t1, t2, t3))
  }
}
