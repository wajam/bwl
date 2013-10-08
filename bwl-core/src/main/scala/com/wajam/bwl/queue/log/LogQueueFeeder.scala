package com.wajam.bwl.queue.log

import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.{TaskData, TaskContext}
import com.wajam.bwl.queue.{ QueueTask, QueueDefinition, PrioritySelector }
import com.wajam.bwl.utils.PeekIterator
import com.wajam.bwl.queue.log.LogQueueFeeder.QueueReader
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._
import com.wajam.commons.Closable

/**
 * Feeder implementation for the LogQueue persistent queue.
 */
class LogQueueFeeder(definition: QueueDefinition, createPriorityReader: (Int, Option[Timestamp]) => QueueReader)
    extends Feeder {

  private val selector = new PrioritySelector(definition.priorities)
  private var readers: Map[Int, QueueReader] = Map()
  private var randomTaskIterator: PeekIterator[Option[QueueEntry.Enqueue]] = PeekIterator(Iterator())

  // Collection of non-acknowledged entries
  private var pendingEntries: Map[Timestamp, QueueEntry.Enqueue] = Map()

  implicit def entry2data(entry: Option[QueueEntry.Enqueue]): Option[TaskData] = {
    entry.map(d => TaskData(d.token,
      Map("token" -> d.token, "id" -> d.id.value, "priority" -> d.priority, "data" -> d.data)))
  }

  def name = definition.name

  def init(context: TaskContext) {
    // TODO: extract start timestamp for each priority from TaskContext and use it to create readers
    readers = definition.priorities.map(priority => priority.value -> createPriorityReader(priority.value, None)).toMap
    randomTaskIterator = PeekIterator(Iterator.continually(readers(selector.next).next()))
  }

  def peek() = {
    if (randomTaskIterator.nonEmpty) {
      randomTaskIterator.peek match {
        case None => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case data => data
      }
    } else {
      None
    }
  }

  def next() = {
    randomTaskIterator.next() match {
      case Some(entry) => {
        pendingEntries += entry.id -> entry
        entry2data(Some(entry))
      }
      case None => None
    }
  }

  def ack(data: TaskData) {
    // TODO: update task context with oldest acknowledged timestamp for the acknowledged priority
    pendingEntries -= data.values(TaskId).toString.toLong
  }

  def kill() {
    readers.valuesIterator.foreach(_.close())
  }

  def pendingEntry(taskId: Timestamp): Option[QueueEntry.Enqueue] = pendingEntries.get(taskId)
}

object LogQueueFeeder {
  type QueueReader = Iterator[Option[QueueEntry.Enqueue]] with Closable
}
