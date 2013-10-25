package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import com.wajam.nrv.service.{ ServiceMember, Service }
import com.wajam.bwl.QueueResource
import com.wajam.bwl.ClosableIterator._
import com.wajam.nrv.cluster.LocalNode
import com.wajam.bwl.queue.QueueItem
import com.wajam.nrv.data.{ InMessage, Message }
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

@RunWith(classOf[JUnitRunner])
class TestTaskItemReader extends FlatSpec {

  trait QueueService {
    val service = new Service("queue")
    val resource = new QueueResource((_, _) => None, (_) => null, (_) => new ServiceMember(0, new LocalNode(Map("nrv" -> 34578))))
    resource.registerTo(service)
  }

  private def task(id: Int): QueueItem = QueueItem.Task(token = id, priority = id, id, data = id)

  private def someTaskMessage(taskId: Int): Option[InMessage] = Some(LogQueue.item2request(task(taskId)))

  private def someAckMessage(ackId: Long, taskId: Int): Option[InMessage] = Some(
    LogQueue.item2request(QueueItem.Ack(token = taskId, priority = taskId, ackId, taskId)))

  "Reader" should "convert task request message to QueueItem.Task" in new QueueService {
    val messages = Iterator(someTaskMessage(1), someTaskMessage(2))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.toList should be(List(Some(task(1)), Some(task(2))))
  }

  it should "filter out processed tasks" in new QueueService {
    val messages = Iterator(someTaskMessage(1), someTaskMessage(2), someTaskMessage(3))
    val reader = PriorityTaskItemReader(service, messages, processed = Set(2L))
    reader.toList should be(List(Some(task(1)), Some(task(3))))
  }

  it should "skip ack messages" in new QueueService {
    val messages = Iterator(someTaskMessage(2), someTaskMessage(3), someAckMessage(4, taskId = 1))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.toList should be(List(Some(task(2)), Some(task(3))))
  }

  it should "not filter out None" in new QueueService {
    val messages = Iterator(None, someTaskMessage(1), None, None, someTaskMessage(2), someTaskMessage(3))
    val reader = PriorityTaskItemReader(service, messages, Set())
    reader.take(3).toList should be(List(None, Some(task(1)), None))
    reader.toList should be(List(None, Some(task(2)), Some(task(3))))
  }

  it should "call adapted Iterator.close() when closed" in new QueueService with MockitoSugar {
    val mockItr = mock[Iterator[Option[Message]]]
    val reader = PriorityTaskItemReader(service, mockItr, Set())
    reader.close()
    verify(mockItr).close()
  }

}
