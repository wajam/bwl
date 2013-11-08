package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.service.Service
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.bwl.QueueResource._
import scala.collection.immutable.TreeMap
import com.wajam.spnl.feeder.Feeder._
import com.wajam.bwl.queue.QueueDefinition
import scala.util.Random

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition)(implicit random: Random = Random) extends Queue {
  self =>

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[QueueItem.Task]).toMap
  private var pendingTasks: Map[Timestamp, QueueItem.Task] = TreeMap()

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  private val totalTaskCount = new AtomicInteger()

  def enqueue(taskItem: QueueItem.Task) = {
    queues(taskItem.priority).offer(taskItem)
    totalTaskCount.incrementAndGet()
    taskItem
  }

  def ack(ackItem: QueueItem.Ack) = ackItem

  lazy val feeder = new Feeder {

    def name = self.name

    def init(context: TaskContext) {
      // No-op. Memory queues are not persisted.
    }

    def peek(): Option[FeederData] = {
      randomTaskIterator.peek match {
        case null => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case item => Some(item.toFeederData)
      }
    }

    def next(): Option[FeederData] = {
      randomTaskIterator.next() match {
        case null => None
        case item => {
          pendingTasks += item.taskId -> item
          Some(item.toFeederData)
        }
      }
    }

    def ack(data: FeederData) {
      val taskId = data(TaskId).toString.toLong
      if(isPending(taskId)) totalTaskCount.decrementAndGet()
      pendingTasks -= taskId
    }

    def kill() {}

    def isPending(taskId: Timestamp): Boolean = pendingTasks.contains(taskId)
  }

  def stats: QueueStats = MemoryQueueStats

  def start() {}

  def stop() {}

  private object MemoryQueueStats extends QueueStats {
    def totalTasks = totalTaskCount.get()

    def pendingTasks = self.pendingTasks.valuesIterator.toIterable

    // TODO: support delayed tasks
    def delayedTasks = Nil
  }
}

object MemoryQueue {
  def create(token: Long, definition: QueueDefinition, service: Service)(implicit random: Random = Random): Queue = {
    new MemoryQueue(token, definition)
  }
}