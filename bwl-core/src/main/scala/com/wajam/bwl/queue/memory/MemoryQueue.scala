package com.wajam.bwl.queue.memory

import scala.util.Random
import scala.collection.immutable.TreeMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import com.wajam.commons.CurrentTime
import com.wajam.nrv.service.Service
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.feeder.Feeder._
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.{ QueueMetrics, PeekIterator, DelayedTaskIterator }
import com.wajam.bwl.queue._
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.QueueDefinition

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition)(implicit random: Random = Random, clock: CurrentTime = new CurrentTime {}) extends Queue with QueueMetrics {
  self =>

  implicit val queueMetrics = this

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[QueueItem.Task]).toMap
  private var pendingTasks: Map[Timestamp, QueueItem.Task] = TreeMap()

  private val delayedTaskIterator = new DelayedTaskIterator(
    Iterator.continually(queues(selector.next).poll()).map(Option(_)), clock)

  private val taskIterator = PeekIterator(delayedTaskIterator)

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
      taskIterator.peek match {
        case Some(item) => Some(item.toFeederData)
        case None => {
          // Peek returned nothing, must skip it or will always be empty
          taskIterator.next()
          None
        }
      }
    }

    def next(): Option[FeederData] = {
      taskIterator.next().map { item =>
        pendingTasks += item.taskId -> item
        item.toFeederData
      }
    }

    def ack(data: FeederData) {
      val taskId = data(TaskId).toString.toLong
      if (isPending(taskId)) totalTaskCount.decrementAndGet()
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

    def pendingTasks = self.pendingTasks.values

    def delayedTasks = delayedTaskIterator.delayedTasks.toIterable
  }
}

object MemoryQueue {

  class Factory(implicit random: Random = Random, clock: CurrentTime = new CurrentTime {}) extends QueueFactory {
    def createQueue(token: Long, definition: QueueDefinition, service: Service, instrument: Boolean = true): Queue = {
      new MemoryQueue(token, definition)
    }
  }
}
