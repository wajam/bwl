package com.wajam.bwl

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{ LocalNode, StaticClusterManager, Cluster }
import com.wajam.nrv.protocol.NrvProtocol
import com.wajam.bwl.queue._
import com.wajam.bwl.queue.memory.MemoryQueue
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.concurrent.duration._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.spnl.{ TaskContext, Spnl }
import org.mockito.Matchers.{ eq => argEquals }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._
import java.io.File
import java.nio.file.Files
import com.wajam.nrv.service.Service
import com.wajam.bwl.queue.log.LogQueue
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import org.apache.commons.io.FileUtils
import scala.util.Random

trait BwlFixture extends CallbackFixture with MockitoSugar {

  import BwlFixture._

  var bwl: Bwl = null
  var cluster: Cluster = null

  def definition: QueueDefinition

  def runWithFixture(test: (BwlFixture) => Any)(implicit queueFactory: QueueFactory) {
    try {
      queueFactory.before()

      val manager = new StaticClusterManager
      cluster = new Cluster(new LocalNode("localhost", Map("nrv" -> 40373)), manager)

      val protocol = new NrvProtocol(cluster.localNode, 5000, 100)
      cluster.registerProtocol(protocol, default = true)

      bwl = new Bwl(definitions = List(definition), createQueue = queueFactory.factory, spnl = new Spnl)
      bwl.applySupport(responseTimeout = Some(3000))
      cluster.registerService(bwl)
      bwl.addMember(0, cluster.localNode)

      cluster.start()

      // Execute the test code
      test(this)
    } finally {
      cluster.stop()
      cluster = null
      bwl = null
      queueFactory.after()
    }
  }

  def newTaskContext = new TaskContext(normalRate = 50, throttleRate = 50)
}

object BwlFixture {

  trait QueueFactory {
    implicit val random = new Random(999)

    type Factory = (Long, QueueDefinition, Service) => Queue

    def name: String

    def before() = {}

    def after() = {}

    def factory: Factory
  }

  def memoryQueueFactory = new QueueFactory {
    val name = "memory"
    val factory: Factory = MemoryQueue.create
  }

  def persistentQueueFactory = new QueueFactory {
    var logDir: File = null
    var factory: Factory = null

    val name = "persistent"

    override def before() = {
      logDir = Files.createTempDirectory("TestBwl").toFile
      factory = LogQueue.create(logDir)
    }

    override def after() = {
      FileUtils.deleteDirectory(logDir)
      logDir = null
    }
  }

  class SpyQueueFactory(queueFactory: QueueFactory) extends QueueFactory {

    private var spyQueues: List[Queue] = Nil

    def allQueues = spyQueues

    def name = queueFactory.name

    override def before() = queueFactory.before()

    override def after() = queueFactory.after()

    def factory = createQueue

    private def createQueue(token: Long, definition: QueueDefinition, service: Service): Queue = {
      val queue = queueFactory.factory(token, definition, service)
      spyQueues = spy[Queue](queue) :: spyQueues
      spyQueues.head
    }
  }
}

trait CallbackFixture extends MockitoSugar {

  trait MockableCallback {
    def process(data: QueueTask.Data)
  }

  val mockCallback: MockableCallback = mock[MockableCallback]

  def taskCallback: QueueTask.Callback
}

abstract class OkCallbackFixture(delay: Long = 0L) extends CallbackFixture {

  import scala.concurrent.future
  import ExecutionContext.Implicits.global

  def taskCallback = (data) => future {
    Thread.sleep(delay)
    mockCallback.process(data)
    QueueTask.Result.Ok
  }
}

abstract class FailCallbackFixture(ignore: Boolean = false, delay: Long = 0L) extends CallbackFixture {
  this: BwlFixture =>

  import scala.concurrent.future
  import ExecutionContext.Implicits.global

  def taskCallback = (data) => future {
    Thread.sleep(delay)
    mockCallback.process(data)
    QueueTask.Result.Fail(new Exception(), ignore)
  }
}

trait SingleQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val definition = QueueDefinition("single", taskCallback, newTaskContext)
}

trait MultipleQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val definition = QueueDefinition("weighted", taskCallback, newTaskContext,
    priorities = List(Priority(1, weight = 66), Priority(2, weight = 33)))
}

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite {

  import BwlFixture._

  def singlePriorityQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - queue should enqueue and dequeue expected value") {
      new OkCallbackFixture with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        f.bwl.enqueue(0, f.definition.name, "hello")
        verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))
        verifyNoMoreInteractions(f.mockCallback)
      })
    }

  }

  def multiplePrioritiesQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - enqueued priority weights should be respected") {
      import ExecutionContext.Implicits.global

      new OkCallbackFixture with BwlFixture with MultipleQueueFixture {}.runWithFixture((f) => {
        val enqueued = f.definition.priorities.flatMap(p => 1.to(200).map(i => {
          f.bwl.enqueue(i, f.definition.name, Map("p" -> p.value, "i" -> i), Some(p.value))
        }))

        Await.result(Future.sequence(enqueued), 5.seconds)

        val dataCaptor = ArgumentCaptor.forClass(classOf[Map[String, Any]])
        verify(f.mockCallback, timeout(5000).atLeast(100)).process(dataCaptor.capture())

        val values = dataCaptor.getAllValues.toList.take(100)
        val p1Count = values.map(_("p")).count(_ == 1)
        val p2Count = values.map(_("p")).count(_ == 2)

        p1Count should (be > 60 and be < 70)
        p2Count should (be > 30 and be < 40)
      })
    }

  }

  testsFor(multiplePrioritiesQueue(memoryQueueFactory))
  testsFor(singlePriorityQueue(memoryQueueFactory))
  testsFor(multiplePrioritiesQueue(persistentQueueFactory))
  testsFor(singlePriorityQueue(persistentQueueFactory))

  test("ok callback should BE akcnowledged") {
    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new OkCallbackFixture with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definition.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(1)
    })
  }

  test("fail callback should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture() with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definition.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      Thread.sleep(100)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

  test("fail ignore callback should BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture(ignore = true) with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.enqueue(0, f.definition.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      verify(spyQueue, timeout(2000)).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(1)
    })
  }

  test("ok callback completed after response timeout should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new OkCallbackFixture(delay = 300L) with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.applySupport(responseTimeout = Some(200L))

      f.bwl.enqueue(0, f.definition.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      // Sleep to give enough time to timeout
      Thread.sleep(500)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

  test("fail ignore callback completed after response timeout should NOT BE acknowledged") {

    implicit val spyQueueFactory = new SpyQueueFactory(memoryQueueFactory)

    new FailCallbackFixture(delay = 300L, ignore = true) with BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
      import ExecutionContext.Implicits.global

      f.bwl.applySupport(responseTimeout = Some(200L))

      f.bwl.enqueue(0, f.definition.name, "hello")
      verify(f.mockCallback, timeout(2000)).process(argEquals("hello"))

      val spyQueue = spyQueueFactory.allQueues.head
      val taskCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Task])
      val ackCaptor = ArgumentCaptor.forClass(classOf[QueueItem.Ack])
      verify(spyQueue, timeout(2000)).enqueue(taskCaptor.capture())
      taskCaptor.getAllValues.size() should be(1)

      // Sleep to give enough time to timeout
      Thread.sleep(500)
      verify(spyQueue, never()).ack(ackCaptor.capture())
      ackCaptor.getAllValues.size() should be(0)
    })
  }

}
