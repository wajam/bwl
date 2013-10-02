package com.wajam.bwl

import com.wajam.nrv.extension.resource._
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.queue.Queue
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.InvalidParameter
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.nrv.data.InMessage
import QueueResource._
import com.wajam.nrv.service.ServiceMember
import com.wajam.commons.SynchronizedIdGenerator

class QueueResource(getQueue: => (Long, String) => Option[Queue], getMember: Long => ServiceMember)
    extends Resource("queues/:token/:name/tasks", "id") with Create with Delete {

  private val timestampGenerator = new TimestampIdGenerator with SynchronizedIdGenerator[Long]

  protected def create = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => {

        // Ensure message has a timestamp
        val timestamp: Timestamp = message.timestamp match {
          case Some(ts) => ts
          case None => {
            val ts: Timestamp = timestampGenerator.nextId
            message.timestamp = Some(ts)
            ts
          }
        }

        params.optionalParam[Int](Priority) match {
          case Some(priority) => {
            queue.enqueue(message, priority)
            message.reply(Map(TaskId -> timestamp.toString))
          }
          case None if queue.priorities.size == 1 => {
            // If no priority is specified and queue has only one priority, default to that priority
            queue.enqueue(message, queue.priorities.head.value)
            message.reply(Map(TaskId -> timestamp.toString))
          }
          case None => throw new InvalidParameter("Parameter priority must be specified")
        }
      }
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }

  protected def delete = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)
    val id = params.param[Long](TaskId)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => queue.ack(id, message)
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }
}

object QueueResource {
  val QueueName = "name"
  val TaskToken = "token"
  val TaskId = "id"
  val Priority = "priority"
}
