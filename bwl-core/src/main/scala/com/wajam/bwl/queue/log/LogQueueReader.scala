package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import scala.collection.mutable
import com.wajam.bwl.queue.QueueService
import com.wajam.nrv.service.Service
import com.wajam.commons.Closable

/**
 * Reader which returns only unprocessed tasks. Tasks present in the `processed` set are skip and the set updated.
 * This set is initialized by reading the logs to the end before creating this reader.
 */
class LogQueueReader(service: QueueService with Service, itr: Iterator[Option[Message]] with Closable,
                     processed: mutable.Set[Timestamp]) extends Iterator[Option[QueueEntry.Enqueue]] with Closable {

  import QueueEntry.message2entry

  val enqueueEntries: Iterator[Option[QueueEntry.Enqueue]] = itr.map {
    case Some(msg) => message2entry(msg, service)
    case None => None
  }.collect {
    case Some(data: QueueEntry.Enqueue) => Some(data)
    case None => None
  }.withFilter {
    case Some(data: QueueEntry.Enqueue) => processed.remove(data.id)
    case None => true
  }

  def hasNext = enqueueEntries.hasNext

  def next() = enqueueEntries.next()

  def close() {
    itr.close()
  }
}
