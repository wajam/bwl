package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.Closable
import scala.collection.mutable
import com.wajam.bwl.queue.QueueService
import com.wajam.nrv.service.Service

class LogQueueReader(service: QueueService with Service, itr: Iterator[Option[Message]] with Closable,
                     processed: mutable.Set[Timestamp]) extends Iterator[Option[QueueEntry.Enqueue]] with Closable {

  import QueueEntry.message2LogTask

  val enqueueEntries: Iterator[Option[QueueEntry.Enqueue]] = itr.map {
    case Some(msg) => message2LogTask(msg, service)
    case None => None
  }.collect {
    case Some(data: QueueEntry.Enqueue) => {
      processed.remove(data.id)
      Some(data)
    }
    case None => None
  }

  def hasNext = enqueueEntries.hasNext

  def next() = enqueueEntries.next()

  def close() {
    itr.close()
  }
}
