package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{ InMessage, Message }
import java.util.concurrent.ConcurrentLinkedQueue
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.PeekIterator
import com.wajam.nrv.service.Service

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val definition: QueueDefinition) extends Queue {

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[Message]).toMap

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  def enqueue(taskMsg: InMessage, priority: Int) {
    queues(priority).offer(taskMsg)
  }

  def ack(id: Timestamp, ackMsg: InMessage) {
    // No-op. Memory queues are not persisted.
  }

  lazy val feeder = new Feeder {
    implicit def msg2data(message: Message): Option[Map[String, Any]] = {
      Some(Map(
        "token" -> message.token.toString,
        "id" -> message.timestamp.get.value,
        "data" -> message.getData))
    }

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
        case data => data
      }
    }

    def next() = randomTaskIterator.next()

    def ack(data: Map[String, Any]) {
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