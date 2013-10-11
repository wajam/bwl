package com.wajam.bwl

import com.wajam.nrv.extension.resource._
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.queue.{ QueueItem, Queue }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.InvalidParameter
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.nrv.data.InMessage
import QueueResource._
import com.wajam.nrv.service.ServiceMember
import com.wajam.commons.SynchronizedIdGenerator

private[bwl] class QueueResource(getQueue: => (Long, String) => Option[Queue], getMember: Long => ServiceMember)
    extends Resource("queues/:token/:name/tasks", "id") with Create with Delete {

  private val timestampGenerator = new TimestampIdGenerator with SynchronizedIdGenerator[Long]

  protected def create = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => {

        val taskId: Timestamp = message.timestamp.getOrElse(timestampGenerator.nextId)
        val queueItem = params.optionalParam[Int](TaskPriority) match {
          case Some(priority) => {
            QueueItem.Task(taskId, taskToken, priority, message.getData[Any])
          }
          case None if queue.priorities.size == 1 => {
            // If no priority is specified and queue has only one priority, default to that priority
            QueueItem.Task(taskId, taskToken, queue.priorities.head.value, message.getData[Any])
          }
          case None => throw new InvalidParameter("Parameter priority must be specified")
        }
        queue.enqueue(queueItem)
        message.reply(Map(TaskId -> taskId.toString))

      }
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }

  protected def delete = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)
    val taskId = params.param[Long](TaskId)
    val ackId: Timestamp = message.timestamp.getOrElse(timestampGenerator.nextId)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => queue.ack(QueueItem.Ack(ackId, taskId))
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }
}

object QueueResource {
  val QueueName = "name"
  val TaskToken = "token"
  val TaskId = "id"
  val TaskPriority = "priority"
}
