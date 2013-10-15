package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import scala.collection.mutable
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.service.Service
import com.wajam.commons.Closable

/**
 * Readers which returns unprocessed tasks
 */
trait LogQueueReader extends Iterator[Option[QueueItem.Task]] with Closable {
  /**
   * Returns delayed task items ordered from the oldest to the newest tasks
   */
  def delayedTasks: Iterable[QueueItem.Task]
}

object LogQueueReader {
  /**
   * Creates a reader which returns only unprocessed tasks. Tasks present in the `processed` set are skip and the set updated.
   * This set is initialized by reading the logs to the end before creating this reader.
   */
  def apply(service: Service, itr: Iterator[Option[Message]] with Closable,
            processed: mutable.Set[Timestamp]): LogQueueReader = {
    new LogQueueReader {

      import LogQueue.message2item

      val taskItems: Iterator[Option[QueueItem.Task]] = itr.map {
        case Some(msg) => message2item(msg, service)
        case None => None
      }.collect {
        case Some(data: QueueItem.Task) if !processed.remove(data.taskId) => Some(data)
        case None => None
      }

      def hasNext = taskItems.hasNext

      def next() = taskItems.next()

      def close() {
        itr.close()
      }

      // TODO: support delayed tasks
      def delayedTasks: Iterable[QueueItem.Task] = Nil
    }
  }
}
