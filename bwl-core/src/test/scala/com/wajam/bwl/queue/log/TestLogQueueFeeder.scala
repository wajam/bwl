package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.bwl.queue._
import com.wajam.spnl.TaskContext
import com.wajam.bwl.FeederTestHelper._
import com.wajam.spnl.feeder.Feeder.FeederData
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import scala.util.Random
import com.wajam.commons.ControlableCurrentTime

@RunWith(classOf[JUnitRunner])
class TestLogQueueFeeder extends FlatSpec with MockitoSugar {

  private def task(id: Long, priority: Int = 1, scheduleTime: Option[Long] = None): QueueItem.Task = QueueItem.Task("name", id, priority, id, data = id, scheduleTime = scheduleTime)

  private def someTask(id: Long, priority: Int = 1, scheduleTime: Option[Long] = None): Option[QueueItem.Task] = Some(task(id, priority, scheduleTime))

  private def feederData(id: Long, priority: Int = 1): FeederData = task(id, priority).toFeederData

  private def someFeederData(id: Long, priority: Int = 1): Option[FeederData] = Some(task(id, priority).toFeederData)

  "Feeder" should "returns expected name" in {
    val feeder = new LogQueueFeeder(QueueDefinition("name", mock[QueueCallback]), (_, _) => mock[PriorityTaskItemReader])
    feeder.name should be("name")
  }

  it should "produce expected tasks" in {
    val mockReader = mock[PriorityTaskItemReader]
    when(mockReader.hasNext).thenReturn(true)
    when(mockReader.next()).thenReturn(someTask(1L), None, someTask(2L), someTask(3L), None)
    val feeder = new LogQueueFeeder(QueueDefinition("name", mock[QueueCallback]), (_, _) => mockReader)
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

  it should "respect order of delayed tasks and update context data accordingly" in {
    implicit val timer = new ControlableCurrentTime {}
    val delay = 10000L
    val scheduleTime = timer.currentTime + delay

    val mockReader = mock[PriorityTaskItemReader]
    when(mockReader.hasNext).thenReturn(true)
    when(mockReader.next()).thenReturn(someTask(1L), None, someTask(2L, scheduleTime = Some(scheduleTime)), someTask(3L), None)
    val feeder = new LogQueueFeeder(QueueDefinition("name", mock[QueueCallback]), (_, _) => mockReader)
    val context = TaskContext()
    feeder.init(context)

    // Fetch non-delayed tasks
    feeder.take(6).toList should be(List(someFeederData(1L), None, someFeederData(3L), None, None, None))
    feeder.pendingTasks.toList should be(List(task(1L), task(3L)))
    feeder.isPending(1L) should be(true)

    // Ack non-delayed tasks
    feeder.ack(feederData(1L))
    feeder.ack(feederData(3L))
    feeder.pendingTasks.toList should be(Nil)

    context.data("1") should be(2L)
    feeder.oldestTaskIdFor(1) should be(Some(Timestamp(2L)))

    timer.advanceTime(delay)

    // Fetch the delayed task
    feeder.take(2).toList should be(List(None, someFeederData(2L)))
    feeder.pendingTasks.toList should be(List(task(2L, scheduleTime = Some(scheduleTime))))
    feeder.isPending(2L) should be(true)

    // Ack the delayed task
    feeder.ack(feederData(2L))
    feeder.pendingTasks.toList should be(Nil)

    context.data("1") should be(3L)
    feeder.oldestTaskIdFor(1) should be(Some(Timestamp(3L)))
  }

  it should "start at context position" in {

    val expectedStartTimestamp: Timestamp = 5L
    var actualStartTimestamp: Option[Timestamp] = None

    val mockReader = mock[PriorityTaskItemReader]
    when(mockReader.next()).thenReturn(None)
    def createReader(priority: Int, startTimestamp: Option[Timestamp]): PriorityTaskItemReader = {
      actualStartTimestamp = startTimestamp
      mockReader
    }

    val feeder = new LogQueueFeeder(QueueDefinition("name", mock[QueueCallback]), createReader)
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
    val feeder = new LogQueueFeeder(QueueDefinition("name", mock[QueueCallback], priorities = priorities), createReader)
    val context = TaskContext()
    feeder.init(context)

    // take(99) results to 100 `next()` calls because feeder is peekable and reads one task ahead
    feeder.take(99).toList

    answers(1).callsCount should be(63)
    answers(2).callsCount should be(37)
  }
}
