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

  val dummyCallback: QueueTask.Callback = (_) => mock[Future[QueueTask.Result]]

  def toTask(id: Long, priority: Int = 1): QueueItem.Task = QueueItem.Task(id, id, priority, id)

  def toSomeTask(id: Long, priority: Int = 1): Option[QueueItem.Task] = Some(toTask(id, priority))

  def toFeederData(id: Long, priority: Int = 1): FeederData = toTask(id, priority).toFeederData

  def toSomeFeederData(id: Long, priority: Int = 1): Option[FeederData] = Some(toTask(id, priority).toFeederData)

  "Feeder" should "returns expected name" in {
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), (_, _) => mock[LogQueueReader])
    feeder.name should be("name")
  }

  it should "produce expected tasks" in {
    val mockReader = mock[LogQueueReader]
    when(mockReader.hasNext).thenReturn(true)
    when(mockReader.next()).thenReturn(toSomeTask(1L), None, toSomeTask(2L), toSomeTask(3L), None)
    when(mockReader.delayedTasks).thenReturn(Nil)
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), (_, _) => mockReader)
    val context = TaskContext()
    feeder.init(context)

    // Verify peek
    feeder.peek() should be(toSomeFeederData(1L))
    feeder.peek() should be(toSomeFeederData(1L))
    feeder.pendingTasks.toList should be(List())
    feeder.pendingTaskPriorityFor(1L) should be(None)

    // Verify feeder produced data and state
    feeder.take(6).toList should be(List(toSomeFeederData(1L), None, toSomeFeederData(2L), toSomeFeederData(3L), None, None))
    feeder.pendingTasks.toList should be(List(toTask(1L), toTask(2L), toTask(3L)))
    feeder.pendingTaskPriorityFor(1L) should be(Some(1))

    // Verify feeder state after acknowledging the first task
    feeder.ack(toFeederData(1L))
    feeder.pendingTasks.toList should be(List(toTask(2L), toTask(3L)))
    feeder.pendingTaskPriorityFor(1L) should be(None)
    context.data("1") should be(2L) // priority position

    // Verify feeder state after acknowledging the last task
    feeder.ack(toFeederData(3L))
    feeder.pendingTasks.toList should be(List(toTask(2L)))
    context.data("1") should be(2L) // priority position

    // Verify feeder state after acknowledging the middle task
    feeder.ack(toFeederData(2L))
    feeder.pendingTasks.toList should be(List())
    context.data("1") should be(3L) // priority position
  }

  it should "start at context position" in {

    val expectedStartTimestamp: Timestamp = 5L
    var actualStartTimestamp: Option[Timestamp] = None

    val mockReader = mock[LogQueueReader]
    def createReader(priority: Int, startTimestamp: Option[Timestamp]): LogQueueReader = {
      actualStartTimestamp = startTimestamp
      mockReader
    }

    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback), createReader)
    val context = TaskContext(data = Map("1" -> expectedStartTimestamp.value))
    feeder.init(context)

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
    def createReader(priority: Int, startTimestamp: Option[Timestamp]): LogQueueReader = {
      val nextAnswer = new NextAnswerCounter(priority)
      answers += priority -> nextAnswer

      val mockReader = mock[LogQueueReader]
      when(mockReader.hasNext).thenReturn(true)
      when(mockReader.next()).thenAnswer(nextAnswer)
      mockReader
    }

    val priorities = List(Priority(1, weight = 66), Priority(2, weight = 33))
    implicit val random = new Random(seed = 999)
    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback, priorities = priorities), createReader)
    val context = TaskContext()
    feeder.init(context)

    // take(99) results to 100 next() calls because feeder is peekable and reads one task ahead
    feeder.take(99).toList

    answers(1).callsCount should be(63)
    answers(2).callsCount should be(37)
  }

  it should "returns expected delayed tasks" in {
    val priorities = List(Priority(1, 1), Priority(2, 2))
    val priority1DelayedTasks = List(toTask(id = 1, priority = 1), toTask(id = 3, priority = 1))
    val priority2DelayedTasks = List(toTask(id = 2, priority = 2))

    val mockReader1 = mock[LogQueueReader]
    when(mockReader1.delayedTasks).thenReturn(priority1DelayedTasks)
    val mockReader2 = mock[LogQueueReader]
    when(mockReader2.delayedTasks).thenReturn(priority2DelayedTasks)

    def createReader(priority: Int, startTimestamp: Option[Timestamp]): LogQueueReader = priority match {
      case 1 => mockReader1
      case 2 => mockReader2
    }

    val feeder = new LogQueueFeeder(QueueDefinition("name", dummyCallback, priorities = priorities), createReader)
    val context = TaskContext()
    feeder.init(context)

    feeder.delayedTasks.toList should be(priority1DelayedTasks ++ priority2DelayedTasks)
  }
}
