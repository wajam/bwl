package com.wajam.bwl.queue.log

import com.wajam.bwl.queue.QueueItem
import com.wajam.commons.Closable
import com.wajam.nrv.consistency.replication.ReplicationSourceIterator
import com.wajam.bwl.queue.log.LogQueue._
import com.wajam.nrv.service.Service
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.utils.timestamp.Timestamp

// TODO: document this!!!
class QueueItemReader(service: Service, maxItemId: Timestamp, prioritySources: Map[Int, ReplicationSourceIterator]) extends Iterator[QueueItem] with Closable {

  val priorityItems: Map[Int, PeekIterator[QueueItem]] = prioritySources.map(e => e._1 -> toPeekIterator(e._2))

  def hasNext = priorityItems.valuesIterator.exists(_.hasNext)

  def next() = {
    val nextItr: PeekIterator[QueueItem] = priorityItems.valuesIterator.withFilter(_.hasNext).reduce(QueueItem.IteratorOrdering.min)
    nextItr.next()
  }

  def close() = prioritySources.valuesIterator.foreach(_.close())

  def toPeekIterator(itr: ReplicationSourceIterator): PeekIterator[QueueItem] = {
    implicit val ord = Ordering[Option[Timestamp]]
    val maxTimestamp = Some(maxItemId)
    val items: Iterator[QueueItem] = itr.takeWhile(
      msg => msg.isDefined && msg.get.timestamp <= maxTimestamp).flatten.flatMap(message2item(_, service))
    PeekIterator(items)
  }
}