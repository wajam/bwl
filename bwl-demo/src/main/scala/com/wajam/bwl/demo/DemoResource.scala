package com.wajam.bwl.demo

import com.wajam.nrv.extension.resource._
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.Bwl
import com.wajam.nrv.data.{ MValue, InMessage }
import scala.concurrent.ExecutionContext
import com.wajam.commons.Logging
import com.wajam.bwl.queue.{ QueueDefinition, QueueTask }
import scala.compat.Platform.EOL

class DemoResource(bwl: Bwl, definitions: Iterable[QueueDefinition])(implicit ec: ExecutionContext)
    extends Resource("queues", "name") with Update with Index with Logging {

  protected def index = (message: InMessage) => {
    val data = definitions.map { definition =>
      s"${definition.name} ${definition.priorities.map(_.value).mkString("[", ",", "]")}"
    }.mkString("", EOL, EOL)
    respond(message, data)
  }

  protected def update = (message: InMessage) => {
    val params: ParamsAccessor = message

    info(s"Enqueueing $message")

    val name = params.param[String]("name")
    val priority = params.optionalParam[Int]("priority")
    val data = message.getData[Any]
    val enqueueFuture = bwl.enqueue(message.token, name, data, priority)

    enqueueFuture onSuccess {
      case id => respond(message, id.toString + EOL)
    }

    enqueueFuture onFailure {
      case e => respond(message, e.getStackTraceString + EOL, code = 500)
    }
  }

  def respond(message: InMessage, data: String, code: Int = 200) {
    message.reply(Map(), Map[String, MValue]("Content-Type" -> "text/plain; charset=utf-8"), data, code = 200)
  }
}

object DemoResource {

  class Callback(name: String)(implicit ec: ExecutionContext) extends QueueTask.Callback with Logging {
    def apply(data: QueueTask.Data) = {
      import scala.concurrent._
      future {
        info(s"Queue '$name' callback executed: $data")
        QueueTask.Result.Ok
      }
    }
  }

}