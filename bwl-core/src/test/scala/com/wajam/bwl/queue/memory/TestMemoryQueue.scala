package com.wajam.bwl.queue.memory

import scala.concurrent.Future
import scala.util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import com.wajam.commons.ControlableCurrentTime
import com.wajam.nrv.service.Service
import com.wajam.bwl.queue.{QueueItem, QueueTask}
import com.wajam.bwl.FeederTestHelper._
import com.wajam.bwl.QueueStatsHelper
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestMemoryQueue extends FlatSpec with MockitoSugar {

  private def task(taskId: Long, priority: Int = 1, executeAfter: Option[Timestamp] = None) = QueueItem.Task("name", taskId, priority, taskId, taskId, executeAfter)

  trait WithDefinition {
    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))
    val definition: QueueDefinition = QueueDefinition("name", (_) => mock[Future[QueueTask.Result]], priorities = priorities)
  }

  trait WithQueue extends WithDefinition with MockitoSugar {
    val queue = new MemoryQueue(0, definition) with ControlableCurrentTime
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

  it should "peek without affecting queue state" in new WithQueue {
    // Enqueue
    val t1 = queue.enqueue(task(taskId = 1L, priority = 1))
    val t2 = queue.enqueue(task(taskId = 2L, priority = 1))
    val t3 = queue.enqueue(task(taskId = 3L, priority = 1))

    queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)

    waitForFeederData(queue.feeder)

    queue.feeder.peek() should be(Some(t1.toFeederData))
    queue.feeder.peek() should be(Some(t1.toFeederData))

    queue.stats.verifyEqualsTo(totalTasks = 3, pendingTasks = Nil)
  }

  it should "properly acknowledge the pending tasks" in new WithQueue {
    // Enqueue
    val t1 = queue.enqueue(task(taskId = 1L, priority = 1))
    val t2 = queue.enqueue(task(taskId = 2L, priority = 1))
    val t3 = queue.enqueue(task(taskId = 3L, priority = 1))

    queue.feeder.take(20).flatten.toList should be(List(t1, t2, t3).map(_.toFeederData))

    // Acknowledge t2
    queue.ack(t2.toAck(ackId = 4L))
    queue.feeder.ack(t2.toFeederData)

    queue.stats.verifyEqualsTo(totalTasks = 2, pendingTasks = List(t1, t3))

    // Acknowledge t1
    queue.ack(t1.toAck(ackId = 4L))
    queue.feeder.ack(t1.toFeederData)

    queue.stats.verifyEqualsTo(totalTasks = 1, pendingTasks = List(t3))

    // Acknowledge t3
    queue.ack(t3.toAck(ackId = 4L))
    queue.feeder.ack(t3.toFeederData)

    queue.stats.verifyEqualsTo(totalTasks = 0, pendingTasks = Nil)
  }

  it should "produce expected task priority distribution" in new WithDefinition {
    val random = new Random(seed = 999)
    val queue = MemoryQueue.create(0, definition, mock[Service])(random)

    for(priority <- 1 to 2; i <- 1 to 100) {
      queue.enqueue(task(priority, priority))
    }

    // take(99) results to 100 `next()` calls because feeder is peekable and reads one task ahead
    val items = queue.feeder.take(99).toList.flatten

    items.count(_("data") == 1) should be(62)
    items.count(_("data") == 2) should be(36)
  }

  it should "respect delayed tasks order" in new WithQueue {
    val delay = 10000

    // Enqueue a task with a 10s delay
    val t1 = queue.enqueue(task(taskId = 1L, priority = 1, Some(queue.currentTime + delay)))

    // Enqueue regular tasks
    val t2 = queue.enqueue(task(taskId = 2L, priority = 1))
    val t3 = queue.enqueue(task(taskId = 3L, priority = 1))

    // Enqueue a task with a 5s delay
    val t4 = queue.enqueue(task(taskId = 4L, priority = 1, Some(queue.currentTime + delay / 2)))

    waitForFeederData(queue.feeder, 10000L)

    queue.feeder.peek() should be(Some(t2.toFeederData))
    queue.feeder.next() should be(Some(t2.toFeederData))

    queue.advanceTime(delay)

    // t3 has been fetched before time was advanced
    queue.feeder.peek() should be(Some(t3.toFeederData))
    queue.feeder.next() should be(Some(t3.toFeederData))

    queue.feeder.peek() should be(Some(t1.toFeederData))
    queue.feeder.next() should be(Some(t1.toFeederData))

    queue.feeder.peek() should be(Some(t4.toFeederData))
    queue.feeder.next() should be(Some(t4.toFeederData))
  }
}
