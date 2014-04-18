package com.wajam.bwl.utils

import scala.collection.immutable.List
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import com.wajam.commons.{ CurrentTime, ControlableCurrentTime }
import com.wajam.bwl.queue.QueueItem

@RunWith(classOf[JUnitRunner])
class TestDelayedTaskIterator extends FlatSpec {

  implicit val metrics = new DelayedTaskMetrics with DisabledMetrics

  private def task(taskId: Long, priority: Int = 1, executeAfter: Option[Long] = None) = {
    QueueItem.Task("name", taskId, priority, taskId, taskId, executeAfter)
  }

  private def delayedTaskIterator(tasks: List[Option[QueueItem.Task]], clock: CurrentTime = new CurrentTime {}) = {
    new DelayedTaskIterator(tasks.toIterator, clock)
  }

  "a DelayedTaskIterator" should "return non-delayed tasks in order" in {
    val tasks = List(task(1), task(2), task(3), task(4)).map(Option(_))
    val itr = delayedTaskIterator(tasks)

    itr.take(20).toList should be(tasks)
  }

  it should "return None when wrapped iterator returns None" in {
    val tasks = List(None)
    val itr = delayedTaskIterator(tasks)

    itr.hasNext should be(true)
    itr.next() should be(None)

    itr.hasNext should be(false)
  }

  it should "return None until a delayed task is ready" in {
    val clock = new ControlableCurrentTime {}
    val delay = 10000L

    val tasks = List(task(1, 1, Some(clock.currentTime + delay))).map(Option(_))
    val itr = delayedTaskIterator(tasks, clock)

    itr.hasNext should be(true)
    itr.next() should be(None)
    itr.next() should be(None)
    itr.next() should be(None)

    clock.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "return a delayed task once the delay is elapsed" in {
    val clock = new ControlableCurrentTime {}
    val delay = 10000L

    val tasks = List(task(1, 1, Some(clock.currentTime + delay)), null).map(Option(_))
    val itr = delayedTaskIterator(tasks, clock)

    itr.hasNext should be(true)
    itr.next() should be(None)

    clock.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "respect expected order of delayed/non-delayed tasks" in {
    val clock = new ControlableCurrentTime {}
    val delay = 5000L

    val tasks = List(
      task(1, 1, Some(clock.currentTime + delay * 2)),
      task(2, 1, Some(clock.currentTime + delay)),
      task(3, 1),
      task(4, 1)).map(Option(_))
    val itr = delayedTaskIterator(tasks, clock)

    itr.hasNext should be(true)
    itr.next() should be(tasks(2))

    clock.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(1))

    itr.hasNext should be(true)
    itr.next() should be(tasks(3))

    clock.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "respect expected order of consecutive delayed tasks" in {
    val clock = new ControlableCurrentTime {}
    val delay = 5000L

    val tasks = List(
      task(1, 1, Some(clock.currentTime + delay)),
      task(2, 1, Some(clock.currentTime + delay)),
      task(3, 1, Some(clock.currentTime + delay)),
      task(4, 1, Some(clock.currentTime + delay))).map(Option(_))
    val itr = delayedTaskIterator(tasks, clock)

    itr.hasNext should be(true)
    itr.next() should be(None)
    itr.next() should be(None)
    itr.next() should be(None)

    clock.advanceTime(delay + 3)

    itr.take(20).toList should be(tasks)
  }

  it should "NOT overflow when reading many contiguous delayed tasks" in {
    val clock = new ControlableCurrentTime {}
    val delay = 5000L

    var firstDelayedTask: Option[QueueItem.Task] = None
    var expectedTasks: List[Option[QueueItem.Task]] = Nil
    var taskId = 0L
    val tasks = Iterator.continually[Option[QueueItem.Task]] {
      // Generates 4999 contiguous delayed tasks then 1 non delayed task continuously
      taskId += 1L
      if (taskId % 5000 == 0) {
        val someTask = Some(task(taskId, 1, None))
        expectedTasks = someTask +: expectedTasks
        someTask
      } else {
        val someTask = Some(task(taskId, 1, Some(clock.currentTime + delay)))
        firstDelayedTask = if (firstDelayedTask.isEmpty) someTask else firstDelayedTask
        someTask
      }

    }
    val itr = new DelayedTaskIterator(tasks, clock)

    val actualTasks = itr.take(5).toList
    actualTasks should be(expectedTasks.reverse)
    actualTasks.size should be(5)

    clock.advanceTime(delay + 3)

    itr.hasNext should be(true)
    itr.next() should be(firstDelayedTask)
  }
}
