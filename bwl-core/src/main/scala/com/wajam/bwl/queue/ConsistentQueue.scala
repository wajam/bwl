package com.wajam.bwl.queue

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.commons.Closable

/**
 * Trait for consistent and replicable queue
 */
trait ConsistentQueue extends Queue {
  /**
   * Returns the highest queue item id
   */
  def getLastQueueItemId: Option[Timestamp]

  /**
   * Returns all the queue items between the specified starting and ending item ids inclusively. The queue items are
   * ordered by ascending item id.
   */
  def readQueueItems(startItemId: Timestamp, endItemId: Timestamp): Iterator[QueueItem] with Closable

  /**
   * Add the specified queue item to this queue
   */
  def writeQueueItem(item: QueueItem)

  /**
   * Remove the specified queue item from this queue
   */
  def truncateQueueItem(itemId: Timestamp)
}
