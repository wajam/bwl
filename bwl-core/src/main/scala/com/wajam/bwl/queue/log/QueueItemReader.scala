package com.wajam.bwl.queue.log

import com.wajam.bwl.queue.QueueItem
import com.wajam.commons.Closable
import com.wajam.nrv.consistency.replication.ReplicationSourceIterator
import com.wajam.bwl.queue.log.LogQueue._
import com.wajam.nrv.service.Service
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Multiplex the specified transaction log source iterators, one per priority, into a single log item iterators.
 * Items are ordered by ascending item id.
 */
class QueueItemReader(service: Service, maxItemId: Timestamp, prioritySources: Map[Int, ReplicationSourceIterator])
    extends Iterator[QueueItem] with Closable {

  private val priorityItems: Map[Int, PeekIterator[QueueItem]] =
    prioritySources.map(entry => entry._1 -> messageSource2itemPeekIterator(entry._2))

  def hasNext = priorityItems.valuesIterator.exists(_.hasNext)

  def next() = {
    val nextItr: PeekIterator[QueueItem] = priorityItems.valuesIterator.withFilter(_.hasNext).reduce(QueueItem.IteratorOrdering.min)
    nextItr.next()
  }

  def close() = prioritySources.valuesIterator.foreach(_.close())

  private def messageSource2itemPeekIterator(itr: ReplicationSourceIterator): PeekIterator[QueueItem] = {
    implicit val ord = Ordering[Option[Timestamp]]
    val maxTimestamp = Some(maxItemId)
    val items: Iterator[QueueItem] = itr.takeWhile(
      msg => msg.isDefined && msg.get.timestamp <= maxTimestamp).flatten.flatMap(message2item(_, service))
    PeekIterator(items)
  }
}