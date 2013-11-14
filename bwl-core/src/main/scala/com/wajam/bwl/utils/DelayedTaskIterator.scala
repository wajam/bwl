package com.wajam.bwl.utils

import scala.collection.immutable.TreeMap
import com.wajam.commons.CurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.queue.QueueItem

/**
 * Iterator that wraps an iterator of Tasks and returns tasks with respect to delayed ones.
 * The wrapped iterator should not return null values, and it should not be used outside.
 */
class DelayedTaskIterator(itr: Iterator[Option[QueueItem.Task]], time: CurrentTime) extends Iterator[Option[QueueItem.Task]] {
  // Tasks are indexed with a tuple of (executeAfter, taskId) to ensure both uniqueness and ordering
  private var _delayedTasks: Map[(Long, Timestamp), QueueItem.Task] = TreeMap()

  def delayedTasks = _delayedTasks

  private def isPast(dateTime: Long): Boolean = time.currentTime >= dateTime

  def hasNext = itr.hasNext || _delayedTasks.nonEmpty

  def next(): Option[QueueItem.Task] = {
    _delayedTasks.headOption match {
      case Some(((executeAfter, taskId), task)) if isPast(executeAfter) =>
        // The most urgent delayed task is ready: return it
        _delayedTasks -= ((executeAfter, taskId))
        Some(task)
      case _ if itr.hasNext =>
        // No delayed task ready to be returned: get next from wrapped iterator
        itr.next().flatMap { task =>
          task.executeAfter match {
            case None =>
              // Task is not delayed: return it
              Some(task)
            case Some(executeAfter) if isPast(executeAfter) =>
              // Task is delayed but ready to be executed: return it
              Some(task)
            case Some(executeAfter) =>
              // Task is delayed: save it and go to next
              _delayedTasks += (executeAfter, task.taskId) -> task
              next()
          }
        }
      case _ if _delayedTasks.nonEmpty =>
        // Wrapped iterator is empty but we have delayed tasks: return None
        None
      case _ =>
        // Wrapped iterator is empty and we don't have delayed tasks: error
        throw new NoSuchElementException
    }
  }
}
