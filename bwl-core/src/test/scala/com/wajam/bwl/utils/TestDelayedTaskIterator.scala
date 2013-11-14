package com.wajam.bwl.utils

import scala.collection.immutable.List
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.commons.{CurrentTime, ControlableCurrentTime}
import com.wajam.bwl.queue.QueueItem

@RunWith(classOf[JUnitRunner])
class TestDelayedTaskIterator extends FlatSpec {

  private def task(taskId: Long, priority: Int = 1, executeAfter: Option[Long] = None) = QueueItem.Task("name", taskId, priority, taskId, taskId, executeAfter)

  private def delayedTaskIterator(tasks: List[Option[QueueItem.Task]], timer: CurrentTime = new CurrentTime {}) = new DelayedTaskIterator(tasks.toIterator, timer)

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
    val timer = new ControlableCurrentTime {}
    val delay = 10000L

    val tasks = List(task(1, 1, Some(timer.currentTime + delay))).map(Option(_))
    val itr = delayedTaskIterator(tasks, timer)

    itr.hasNext should be(true)
    itr.next() should be(None)
    itr.next() should be(None)
    itr.next() should be(None)

    timer.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "return a delayed task once the delay is elapsed" in {
    val timer = new ControlableCurrentTime {}
    val delay = 10000L

    val tasks = List(task(1, 1, Some(timer.currentTime + delay)), null).map(Option(_))
    val itr = delayedTaskIterator(tasks, timer)

    itr.hasNext should be(true)
    itr.next() should be(None)

    timer.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "respect expected order of delayed/non-delayed tasks" in {
    val timer = new ControlableCurrentTime {}
    val delay = 5000L

    val tasks = List(
      task(1, 1, Some(timer.currentTime + delay * 2)),
      task(2, 1, Some(timer.currentTime + delay)),
      task(3, 1),
      task(4, 1)
    ).map(Option(_))
    val itr = delayedTaskIterator(tasks, timer)

    itr.hasNext should be(true)
    itr.next() should be(tasks(2))

    timer.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(1))

    itr.hasNext should be(true)
    itr.next() should be(tasks(3))

    timer.advanceTime(delay)

    itr.hasNext should be(true)
    itr.next() should be(tasks(0))

    itr.hasNext should be(false)
  }

  it should "respect expected order of consecutive delayed tasks" in {
    val timer = new ControlableCurrentTime {}
    val delay = 5000L

    val tasks = List(
      task(1, 1, Some(timer.currentTime + delay)),
      task(2, 1, Some(timer.currentTime + delay)),
      task(3, 1, Some(timer.currentTime + delay)),
      task(4, 1, Some(timer.currentTime + delay))
    ).map(Option(_))
    val itr = delayedTaskIterator(tasks, timer)

    itr.hasNext should be(true)
    itr.next() should be(None)
    itr.next() should be(None)
    itr.next() should be(None)

    timer.advanceTime(delay + 3)

    itr.take(20).toList should be(tasks)
  }
}
