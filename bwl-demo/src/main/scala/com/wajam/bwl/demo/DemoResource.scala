package com.wajam.bwl.demo

import com.wajam.nrv.extension.resource._
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.bwl.Bwl
import com.wajam.nrv.data.{ MValue, InMessage }
import scala.concurrent.ExecutionContext
import com.wajam.commons.Logging
import com.wajam.bwl.queue.{ QueueDefinition, QueueCallback }
import scala.compat.Platform.EOL
import com.wajam.nrv.service.Resolver

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
    val data = message.getData[String]
    val token = Resolver.hashData(data)

    val enqueueFuture = bwl.enqueue(token, name, data, priority)

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

  class DemoCallback(name: String, delay: Long = 0) extends QueueCallback with Logging {
    def execute(data: Any)(implicit ec: ExecutionContext) = {
      import scala.concurrent._
      future {
        Thread.sleep(delay)
        info(s"Queue '$name' callback executed: $data after $delay ms")
        QueueCallback.Result.Ok
      }
    }
  }

}