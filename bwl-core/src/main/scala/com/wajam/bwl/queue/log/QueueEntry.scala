package com.wajam.bwl.queue.log

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{ MessageType, Message }
import com.wajam.bwl.queue.QueueService
import com.wajam.nrv.service.Service
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.QueueResource._

sealed trait QueueEntry

object QueueEntry {

  case class Task(id: Timestamp, token: Long, priority: Int, data: Any) extends QueueEntry

  case class Ack(id: Timestamp) extends QueueEntry

  object Response extends QueueEntry

  def message2entry(message: Message, service: QueueService with Service): Option[QueueEntry] = {
    message.function match {
      case MessageType.FUNCTION_CALL if message.path == "/enqueue" => {
        message.timestamp.map(QueueEntry.Task(_, message.token, message.param[Int](TaskPriority), message.getData[Any]))
      }
      case MessageType.FUNCTION_CALL if message.path == "/ack" => message.timestamp.map(QueueEntry.Ack)
      case MessageType.FUNCTION_RESPONSE => Some(QueueEntry.Response)
      case _ => None
    }
  }
}
