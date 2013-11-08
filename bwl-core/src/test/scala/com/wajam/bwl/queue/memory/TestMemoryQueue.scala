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
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.queue.log.{LogQueueFeeder, PriorityTaskItemReader}
import org.mockito.Mockito._
import com.wajam.bwl.queue.Priority
import scala.Some
import com.wajam.bwl.queue.QueueDefinition
import scala.util.Random
import com.wajam.spnl.TaskContext


@RunWith(classOf[JUnitRunner])
class TestMemoryQueue extends FlatSpec with MockitoSugar {

  private val dummyCallback: QueueTask.Callback = (_) => mock[Future[QueueTask.Result]]

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

  it should "produce expected task priority distribution" in {

    class NextAnswerCounter(priority: Int) extends Answer[Option[QueueItem.Task]] {
      var callsCount = 0

      def answer(iom: InvocationOnMock) = {
        callsCount += 1
        None
      }
    }

    var answers: Map[Int, NextAnswerCounter] = Map()
    def createReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = {
      val nextAnswer = new NextAnswerCounter(priority)
      answers += priority -> nextAnswer

      val mockReader = mock[PriorityTaskItemReader]
      when(mockReader.hasNext).thenReturn(true)
      when(mockReader.next()).thenAnswer(nextAnswer)
      mockReader
    }

    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))
    implicit val random = new Random(seed = 999)
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback, priorities = priorities), createReader)
    val context = TaskContext()
    feeder.init(context)

    // take(99) results to 100 `next()` calls because feeder is peekable and reads one task ahead
    feeder.take(99).toList

    answers(1).callsCount should be(63)
    answers(2).callsCount should be(37)
  }
}
