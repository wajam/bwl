package com.wajam.bwl.queue

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.commons.Closable

// TODO: Document this!
trait ConsistentQueue extends Queue {
  def getLastQueueItemId: Option[Timestamp]
  def readQueueItems(fromItemId: Timestamp, toItemId: Timestamp): Iterator[QueueItem] with Closable
  def writeQueueItem(item: QueueItem)
}
