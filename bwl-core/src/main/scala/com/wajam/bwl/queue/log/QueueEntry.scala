package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{ MessageType, Message }
import com.wajam.bwl.queue.QueueService
import com.wajam.nrv.service.{ ActionMethod, Service }
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.QueueResource._

sealed trait QueueEntry

object QueueEntry {

  case class Enqueue(id: Timestamp, token: Long, priority: Int, data: Any) extends QueueEntry

  case class Ack(id: Timestamp, priority: Int) extends QueueEntry

  object Response extends QueueEntry

  def message2LogTask(message: Message, service: QueueService with Service): Option[QueueEntry] = {
    message.function match {
      case MessageType.FUNCTION_CALL => {
        if (service.queueResource.create(service).forall(_.matches(message.path, ActionMethod.POST))) {
          message.timestamp.map(QueueEntry.Enqueue(_, message.token, message.param[Int](TaskPriority), message.getData[Any]))
        } else if (service.queueResource.delete(service).forall(_.matches(message.path, ActionMethod.DELETE))) {
          message.timestamp.map(QueueEntry.Ack(_, message.param[Int](TaskPriority)))
        } else {
          None
        }
      }
      case MessageType.FUNCTION_RESPONSE => Some(QueueEntry.Response)
    }
  }
}
