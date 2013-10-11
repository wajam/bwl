package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import java.util.concurrent.ConcurrentLinkedQueue
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.service.Service
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.QueueDefinition
import com.wajam.spnl.TaskData
import scala.collection.immutable.TreeMap

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition) extends Queue {

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[QueueItem.Task]).toMap
  private var pendingTasks: Map[Timestamp, QueueItem.Task] = TreeMap()

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  def enqueue(taskItem: QueueItem.Task) {
    queues(taskItem.priority).offer(taskItem)
  }

  def ack(ackItem: QueueItem.Ack) {
    // No-op. Memory queues are not persisted.
  }

  lazy val feeder = new Feeder {

    def name = MemoryQueue.this.name

    def init(context: TaskContext) {
      // No-op. Memory queues are not persisted.
    }

    def peek(): Option[TaskData] = {
      import QueueItem.item2data

      randomTaskIterator.peek match {
        case null => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case item => Some(item2data(item))
      }
    }

    def next(): Option[TaskData] = {
      import QueueItem.item2data

      randomTaskIterator.next() match {
        case null => None
        case item => {
          pendingTasks += item.taskId -> item
          Some(item2data(item))
        }
      }
    }

    def ack(data: TaskData) {
      val taskId = data.values(TaskId).toString.toLong
      pendingTasks -= taskId
    }

    def kill() {}
  }

  def stats: QueueStats = MemoryQueueStats

  def start() {}

  def stop() {}

  private object MemoryQueueStats extends QueueStats {
    def totalTasks = queues.valuesIterator.map(_.size()).sum

    def pendingTasks = MemoryQueue.this.pendingTasks.valuesIterator.toIterable

    // TODO: support delayed tasks
    def delayedTasks = Nil
  }
}

object MemoryQueue {
  def create(token: Long, definition: QueueDefinition, service: Service with QueueService): Queue = {
    new MemoryQueue(token, definition)
  }
}