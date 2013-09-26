package com.wajam.bwl.queue.memory

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import java.util.concurrent.ConcurrentLinkedQueue
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.bwl.utils.PeekIterator
import com.wajam.bwl.queue.Priority

/**
 * Simple memory queue. MUST not be used in production.
 */
class MemoryQueue(val token: Long, val name: String, val priorities: Seq[Priority]) extends Queue {

  private val selector = new PrioritySelector(priorities)
  private val queues = priorities.map(_.value -> new ConcurrentLinkedQueue[Message]).toMap

  private val randomTaskIterator = PeekIterator(Iterator.continually(queues(selector.next).poll()))

  def enqueue(task: Message, priority: Int) {
    queues(priority).offer(task)
  }

  def ack(id: Timestamp) {
    // No-op
  }

  lazy val feeder = new Feeder {
    implicit def msg2data(message: Message): Option[Map[String, Any]] = {
      message match {
        case null => None
        case msg => Some(Map(
          "token" -> message.token,
          "id" -> message.timestamp.get.value,
          "data" -> message.getData))
      }
    }

    def name = MemoryQueue.this.name

    def init(context: TaskContext) {}

    def peek() = randomTaskIterator.peek

    def next() = randomTaskIterator.next()

    def ack(data: Map[String, Any]) {}

    def kill() {}
  }

  def start() {}

  def stop() {}
}

object MemoryQueue {
  def apply(token: Long, name: String, priorities: Seq[Priority]): Queue = {
    new MemoryQueue(token, name, priorities)
  }

  def create(token: Long, definition: QueueDefinition): Queue = {
    new MemoryQueue(token, definition.name, definition.priorities)
  }
}