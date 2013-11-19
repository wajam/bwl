package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import com.wajam.bwl.queue.QueueItem
import com.wajam.commons.Closable

/**
 * Readers which returns unprocessed tasks
 */
trait PriorityTaskItemReader extends Iterator[Option[QueueItem.Task]] with Closable

object PriorityTaskItemReader {
  /**
   * Creates a reader which returns only unprocessed tasks. Tasks present in the `processed` set are skip and the set updated.
   * This set is initialized by reading the logs to the end before creating this reader.
   */
  def apply(itr: Iterator[Option[Message]] with Closable,
            processed: Set[Timestamp]): PriorityTaskItemReader = {
    new InternalPriorityTaskItemReader(itr, processed)
  }

  private class InternalPriorityTaskItemReader(itr: Iterator[Option[Message]] with Closable,
                                               var processed: Set[Timestamp]) extends PriorityTaskItemReader {
    import LogQueue.message2item

    lazy val taskItems: Iterator[Option[QueueItem.Task]] = itr.map {
      case Some(msg) => message2item(msg)
      case None => None
    }.collect {
      case Some(task: QueueItem.Task) => Some(task)
      case None => None
    }.withFilter {
      case Some(task: QueueItem.Task) => {
        if (processed.contains(task.taskId)) {
          processed -= task.taskId
          false
        } else true
      }
      case None => true
    }

    def hasNext = taskItems.hasNext

    def next() = taskItems.next()

    def close() = itr.close()

    // TODO: support delayed tasks
    def delayedTasks: Iterable[QueueItem.Task] = Nil
  }
}
