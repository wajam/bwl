package com.wajam.bwl.utils

import scala.annotation.tailrec
import scala.util.Random
import com.wajam.bwl.queue.{ QueueItem, Priority, PrioritySelector }

/**
 * Iterator that fetches tasks from a set of iterators according to their priorities.
 *
 * The PrioritizedIterator always tries to fetch tasks with the priority returned by
 * the PrioritySelector. In case there are no tasks for this priority, it recursively
 * eliminates empty priorities until it finds a non-empty one, so that it never returns
 * None while another priority has tasks.
 */

class PrioritizedIterator(iteratorsByPriority: Map[Int, Iterator[Option[QueueItem.Task]]], allPriorities: Iterable[Priority])(implicit random: Random) extends Iterator[Option[QueueItem.Task]] {

  private val defaultSelector = new PrioritySelector(allPriorities)

  def hasNext = iteratorsByPriority.values.exists(_.hasNext)

  def next() = getNext(defaultSelector, allPriorities)

  @tailrec
  private def getNext(selector: PrioritySelector, priorities: Iterable[Priority]): Option[QueueItem.Task] = {
    val priority = selector.next
    iteratorsByPriority(priority).next() match {
      case s: Some[QueueItem.Task] => s
      case None =>
        // Consider all priorities except the empty ones
        priorities.filterNot(_.value == priority) match {
          case Nil =>
            // All priorities have been tried, return None
            None
          case single :: Nil =>
            // Only one priority left, no need to create a selector
            iteratorsByPriority(single.value).next()
          case reducedPriorities: Iterable[Priority] =>
            // Create a new selector with the reduced set of priorities
            val reducedSelector = new PrioritySelector(reducedPriorities)
            getNext(reducedSelector, reducedPriorities)
        }
    }
  }
}
