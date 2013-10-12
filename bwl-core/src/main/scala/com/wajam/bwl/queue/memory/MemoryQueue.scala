package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import java.util.concurrent.ConcurrentLinkedQueue
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.service.Service
import com.wajam.spnl.feeder.Feeder._
import com.wajam.bwl.queue.QueueDefinition

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition) extends Queue {

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[FeederData]).toMap

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  def enqueue(taskId: Timestamp, taskToken: Long, taskPriority: Int, taskData: Any) {
    val data = Map("token" -> taskToken, "id" -> taskId.value, "priority" -> taskPriority, "data" -> taskData)
    queues(taskPriority).offer(data)
  }

  def ack(ackId: Timestamp, taskId: Timestamp) {
    // No-op. Memory queues are not persisted.
  }

  lazy val feeder = new Feeder {

    def name = MemoryQueue.this.name

    def init(context: TaskContext) {
      // No-op. Memory queues are not persisted.
    }

    def peek() = {
      randomTaskIterator.peek match {
        case null => {
          // Peek returned nothing, must skip it or will always be null
          randomTaskIterator.next()
          None
        }
        case data => Some(data)
      }
    }

    def next() = Option(randomTaskIterator.next())

    def ack(data: FeederData) {
      // No-op. Memory queues are not persisted.
    }

    def kill() {}
  }

  def start() {}

  def stop() {}
}

object MemoryQueue {
  def create(token: Long, definition: QueueDefinition, service: Service with QueueService): Queue = {
    new MemoryQueue(token, definition)
  }
}