package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.bwl.queue.{ Priority, QueueItem, QueueDefinition, QueueTask }
import scala.concurrent.Future
import com.wajam.spnl.TaskContext
import com.wajam.bwl.FeederTestHelper._
import com.wajam.spnl.feeder.Feeder.FeederData
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TestLogQueueFeeder extends FlatSpec with MockitoSugar {

  private val dummyCallback: QueueTask.Callback = (_) => mock[Future[QueueTask.Result]]

  private def task(id: Long, priority: Int = 1): QueueItem.Task = QueueItem.Task("name", token = id, priority, id, data = id)

  private def someTask(id: Long, priority: Int = 1): Option[QueueItem.Task] = Some(task(id, priority))

  private def feederData(id: Long, priority: Int = 1): FeederData = task(id, priority).toFeederData

  private def someFeederData(id: Long, priority: Int = 1): Option[FeederData] = Some(task(id, priority).toFeederData)

  "Feeder" should "returns expected name" in {
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), (_, _) => mock[PriorityTaskItemReader])
    feeder.name should be("name")
  }

  it should "produce expected tasks" in {
    val mockReader = mock[PriorityTaskItemReader]
    when(mockReader.hasNext).thenReturn(true)
    when(mockReader.next()).thenReturn(someTask(1L), None, someTask(2L), someTask(3L), None)
    when(mockReader.delayedTasks).thenReturn(Nil)
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), (_, _) => mockReader)
    val context = TaskContext()
    feeder.init(context)

    // Verify peek
    feeder.peek() should be(someFeederData(1L))
    feeder.peek() should be(someFeederData(1L))
    feeder.pendingTasks.toList should be(List())
    feeder.isPending(1L) should be(false)

    // Verify feeder produced data and state
    feeder.take(6).toList should be(List(someFeederData(1L), None, someFeederData(2L), someFeederData(3L), None, None))
    feeder.pendingTasks.toList should be(List(task(1L), task(2L), task(3L)))
    feeder.isPending(1L) should be(true)

    // Verify feeder state after acknowledging the first task
    feeder.ack(feederData(1L))
    feeder.pendingTasks.toList should be(List(task(2L), task(3L)))
    feeder.isPending(1L) should be(false)
    context.data("1") should be(2L) // priority position in task context
    feeder.oldestTaskIdFor(1) should be(Some(Timestamp(2L)))

    // Verify feeder state after acknowledging the last task
    feeder.ack(feederData(3L))
    feeder.pendingTasks.toList should be(List(task(2L)))
    context.data("1") should be(2L) // priority position in task context
    feeder.oldestTaskIdFor(1) should be(Some(Timestamp(2L)))

    // Verify feeder state after acknowledging the middle task
    feeder.ack(feederData(2L))
    feeder.pendingTasks.toList should be(List())
    context.data("1") should be(3L) // priority position in task context
    feeder.oldestTaskIdFor(1) should be(Some(Timestamp(3L)))
  }

  it should "start at context position" in {

    val expectedStartTimestamp: Timestamp = 5L
    var actualStartTimestamp: Option[Timestamp] = None

    val mockReader = mock[PriorityTaskItemReader]
    def createReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = {
      actualStartTimestamp = startTimestamp
      mockReader
    }

    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), createReader)
    val context = TaskContext(data = Map("1" -> expectedStartTimestamp.value))
    feeder.init(context)
    feeder.oldestTaskIdFor(1) should be(Some(expectedStartTimestamp))

    actualStartTimestamp should be(Some(expectedStartTimestamp))
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

  it should "returns expected delayed tasks" in {
    val priorities = List(Priority(1, 1), Priority(2, 2))
    val priority1DelayedTasks = List(task(id = 1, priority = 1), task(id = 3, priority = 1))
    val priority2DelayedTasks = List(task(id = 2, priority = 2))

    val mockReader1 = mock[PriorityTaskItemReader]
    when(mockReader1.delayedTasks).thenReturn(priority1DelayedTasks)
    val mockReader2 = mock[PriorityTaskItemReader]
    when(mockReader2.delayedTasks).thenReturn(priority2DelayedTasks)

    def createReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = priority match {
      case 1 => mockReader1
      case 2 => mockReader2
    }

    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback, priorities = priorities), createReader)
    val context = TaskContext()
    feeder.init(context)

    feeder.delayedTasks.toList should be(priority1DelayedTasks ++ priority2DelayedTasks)
  }
}
