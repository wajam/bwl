package com.wajam.bwl.queue.log

import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.queue.{ QueueTask, QueueDefinition, PrioritySelector }
import com.wajam.bwl.utils.PeekIterator
import com.wajam.bwl.queue.log.LogQueueFeeder.PriorityReader
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.Closable
import com.wajam.bwl.QueueResource._

class LogQueueFeeder(definition: QueueDefinition, createPriorityReader: (Int, Option[Timestamp]) => PriorityReader)
    extends Feeder {

  private val selector = new PrioritySelector(definition.priorities)
  private var readers: Map[Int, PriorityReader] = Map()
  private var randomTaskIterator: PeekIterator[Option[QueueEntry.Enqueue]] = PeekIterator(Iterator())

  // Collection of non-acknowledged entries
  private var pendingEntries: Map[Timestamp, QueueEntry.Enqueue] = Map()

  implicit def entry2data(entry: Option[QueueEntry.Enqueue]): Option[QueueTask.Data] = {
    entry.map(d => Map("token" -> d.token.toString, "id" -> d.id.value, "priority" -> d.priority, "data" -> d.data))
  }

  def name = definition.name

  def init(context: TaskContext) {
    // TODO: extract start timestamp for each priority from TaskContext and use it to create readers
    readers = definition.priorities.map(priority => priority.value -> createPriorityReader(priority.value, None)).toMap
    randomTaskIterator = PeekIterator(readers(selector.next))
  }

  def peek() = {
    randomTaskIterator.peek match {
      case None => {
        // Peek returned nothing, must skip it or will always be null
        randomTaskIterator.next()
        None
      }
      case data => data
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

  def ack(data: Map[String, Any]) {
    // TODO: update task context with oldest acknowledged timestamp for the acknowledged priority
    pendingEntries -= data(TaskId).toString.toLong
  }

  def kill() {
    readers.valuesIterator.foreach(_.close())
  }

  def pendingEntry(taskId: Timestamp): Option[QueueEntry.Enqueue] = pendingEntries.get(taskId)
}

object LogQueueFeeder {
  type PriorityReader = PeekIterator[Option[QueueEntry.Enqueue]] with Closable
}
