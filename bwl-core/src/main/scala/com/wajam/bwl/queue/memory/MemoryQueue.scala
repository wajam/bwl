package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import java.util.concurrent.ConcurrentLinkedQueue
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.{ TaskData, TaskContext }
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.service.Service

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition) extends Queue {

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[TaskData]).toMap

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  def enqueue(taskItem: QueueItem.Task) {
    val data = TaskData(taskItem.token,
      Map("token" -> taskItem.token, "id" -> taskItem.taskId.value, "priority" -> taskItem.priority, "data" -> taskItem.data))
    queues(taskItem.priority).offer(data)
  }

  def ack(ackItem: QueueItem.Ack) {
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

    def ack(data: TaskData) {
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