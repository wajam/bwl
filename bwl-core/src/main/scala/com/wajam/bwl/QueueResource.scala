package com.wajam.bwl

import com.wajam.nrv.extension.resource._
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.queue.{ QueueDefinition, QueueItem, Queue }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.InvalidParameter
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.nrv.data.{ Message, InMessage }
import QueueResource._
import com.wajam.nrv.service.ServiceMember
import com.wajam.commons.SynchronizedIdGenerator

class QueueResource(getQueue: => (Long, String) => Option[Queue], getDefinition: => (String) => QueueDefinition,
                    getMember: Long => ServiceMember)
    extends Resource("queues/:token/:name/tasks", "id") with Create with Delete {

  private val timestampGenerator = new TimestampIdGenerator with SynchronizedIdGenerator[Long]

  protected def create = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => {
        val taskItem = queue.enqueue(message2task(params))
        message.reply(Map(TaskId -> taskItem.taskId.toString))
      }
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }

  protected def delete = (message: InMessage) => {
    val params: ParamsAccessor = message

    val taskToken = params.param[Long](TaskToken)
    val memberToken = getMember(taskToken).token
    val queueName = params.param[String](QueueName)

    getQueue(memberToken, queueName) match {
      case Some(queue: Queue) => queue.ack(message2ack(params))
      case None => throw new InvalidParameter(s"No queue '$queueName' for shard $memberToken")
    }
  }

  def message2task(params: ParamsAccessor): QueueItem.Task = {
    val taskId: Timestamp = params.message.timestamp.getOrElse(timestampGenerator.nextId)
    val taskToken = params.param[Long](TaskToken)
    val queueName = params.param[String](QueueName)

    params.optionalParam[Int](TaskPriority) match {
      case Some(priority) => {
        QueueItem.Task(taskId, taskToken, priority, params.message.getData[Any])
      }
      case None if getDefinition(queueName).priorities.size == 1 => {
        // If no priority is specified and queue has only one priority, default to that priority
        QueueItem.Task(taskId, taskToken, getDefinition(queueName).priorities.head.value, params.message.getData[Any])
      }
      case None => throw new InvalidParameter(s"Parameter priority must be specified: ${params.message.path}")
    }
  }

  def message2ack(params: ParamsAccessor): QueueItem.Ack = {
    val taskId = params.param[Long](TaskId)
    val ackId: Timestamp = params.message.timestamp.getOrElse(timestampGenerator.nextId)
    val taskToken = params.param[Long](TaskToken)

    QueueItem.Ack(ackId, taskId, taskToken)
  }
}

object QueueResource {
  val QueueName = "name"
  val TaskToken = "token"
  val TaskId = "id"
  val TaskPriority = "priority"
}
