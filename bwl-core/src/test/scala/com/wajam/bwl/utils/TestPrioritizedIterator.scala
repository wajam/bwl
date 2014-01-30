package com.wajam.bwl.utils

import scala.util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import com.wajam.bwl.queue.{ Priority, QueueItem }

@RunWith(classOf[JUnitRunner])
class TestPrioritizedIterator extends FlatSpec {

  implicit val random = new Random(seed = 999)

  def someTask(priority: Int) = Some(QueueItem.Task("name", 0L, priority, 0L, 0L, None))

  trait withTwoPriorities {
    val allpriorities = List(Priority(1, 90), Priority(2, 10))
  }

  trait withThreePriorities {
    val allpriorities = List(Priority(1, 45), Priority(2, 45), Priority(3, 10))
  }

  trait withNonEmptyIterators extends withTwoPriorities {
    val iteratorsByPriority = allpriorities.map { priority =>
      priority.value -> Iterator.continually(someTask(priority.value))
    }.toMap
  }

  trait withOneEmptyIterator extends withTwoPriorities {
    val iteratorsByPriority = Map(
      1 -> (Iterator(someTask(1)) ++ Iterator.continually(None)),
      2 -> Iterator.continually(someTask(2)))
  }

  trait withTwoEmptyIterators extends withThreePriorities {
    val iteratorsByPriority = Map(
      1 -> Iterator.continually(None),
      2 -> (Iterator(someTask(2)) ++ Iterator.continually(None)),
      3 -> Iterator.continually(someTask(3)))
  }

  "a PrioritizedIterator" should "respect priority distribution when no iterator is empty" in new withNonEmptyIterators {
    val iterator = new PrioritizedIterator(iteratorsByPriority, allpriorities)

    val items = iterator.take(100).toList.flatten

    items.count(_.priority == 1) should be(91)
    items.count(_.priority == 2) should be(9)
  }

  it should "fallback on lowest priority when highest priority is empty" in new withOneEmptyIterator {
    val iterator = new PrioritizedIterator(iteratorsByPriority, allpriorities)

    val items = iterator.take(100).toList.flatten

    items.count(_.priority == 1) should be(1)
    items.count(_.priority == 2) should be(99)
  }

  it should "fallback on lowest priority when first two highest priorities are empty" in new withTwoEmptyIterators {
    val iterator = new PrioritizedIterator(iteratorsByPriority, allpriorities)

    val items = iterator.take(100).toList.flatten

    items.count(_.priority == 1) should be(0)
    items.count(_.priority == 2) should be(1)
    items.count(_.priority == 3) should be(99)
  }

}
