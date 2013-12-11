package com.wajam.bwl.utils

import scala.util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.bwl.queue.{ Priority, QueueItem }

@RunWith(classOf[JUnitRunner])
class TestPrioritizedIterator extends FlatSpec {

  implicit val random = new Random(seed = 999)

  val allpriorities = List(Priority(1, 90), Priority(2, 10))

  def someTask(priority: Int) = Some(QueueItem.Task("name", 0L, priority, 0L, 0L, None))

  trait withNonEmptyIterators {
    val iteratorsByPriority = allpriorities.map { priority =>
      priority.value -> Iterator.continually(someTask(priority.value))
    }.toMap
  }

  trait withOneEmptyIterator {
    val iteratorsByPriority = Map(
      1 -> Iterator.continually(None),
      2 -> Iterator.continually(someTask(2)))
  }

  "a PrioritizedIterator" should "respect priority distribution when no iterator is empty" in new withNonEmptyIterators {
    val iterator = new PrioritizedIterator(iteratorsByPriority, allpriorities)

    val items = iterator.take(100).toList.flatten

    items.count(_.priority == 1) should be(91)
    items.count(_.priority == 2) should be(9)
  }

  it should "fallback on low priority when high priority is empty" in new withOneEmptyIterator {
    val iterator = new PrioritizedIterator(iteratorsByPriority, allpriorities)

    val items = iterator.take(100).toList.flatten

    items.count(_.priority == 1) should be(0)
    items.count(_.priority == 2) should be(100)
  }

}
