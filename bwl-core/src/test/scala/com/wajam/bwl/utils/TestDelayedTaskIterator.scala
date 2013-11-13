package com.wajam.bwl.utils

import scala.collection.mutable.Queue
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.commons.ControlableCurrentTime
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestDelayedTaskIterator extends FlatSpec {

  private def task(taskId: Long, priority: Int = 1, executeAfter: Option[Long] = None) = QueueItem.Task("name", taskId, priority, taskId, taskId, executeAfter)

  "a DelayedTaskIterator" should "return non-delayed tasks in order" in {
    val tasks = Queue(task(1), task(2), task(3), task(4)).map(Option(_))
    val itr = new DelayedTaskIterator(tasks.toIterator, new ControlableCurrentTime {})

    itr.next() should be(tasks(0))
    itr.next() should be(tasks(1))
    itr.next() should be(tasks(2))
    itr.next() should be(tasks(3))
  }

  it should "return a delayed task once the delay is elapsed" in {
    val timer = new ControlableCurrentTime {}
    val delay = 10000L

    val tasks = Queue(task(1, 1, Some(timer.currentTime + delay)), null).map(Option(_))
    val itr = new DelayedTaskIterator(tasks.toIterator, timer)

    itr.next() should be(None)

    timer.advanceTime(delay)

    itr.next() should be(tasks(0))
  }

  it should "respect expected delayed/non-delayed tasks order" in {
    val timer = new ControlableCurrentTime {}
    val delay = 5000L

    val tasks = Queue(
      task(1, 1, Some(timer.currentTime + delay * 2)),
      task(2, 1, Some(timer.currentTime + delay)),
      task(3, 1),
      task(4, 1)
    ).map(Option(_))
    val itr = new DelayedTaskIterator(tasks.toIterator, timer)

    itr.next() should be(tasks(2))

    timer.advanceTime(delay)

    itr.next() should be(tasks(1))

    itr.next() should be(tasks(3))

    timer.advanceTime(delay)

    itr.next() should be(tasks(0))
  }

}
