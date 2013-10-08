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

trait BwlFixture extends MockitoSugar {

  import BwlFixture._

  var bwl: Bwl = null
  var cluster: Cluster = null
  var mockCallback: Callback = null

  def definition: QueueDefinition

  def runWithFixture(test: (BwlFixture) => Any)(implicit queueFactory: QueueFactory) {
    try {
      queueFactory.before()
      mockCallback = mock[Callback]

      val manager = new StaticClusterManager
      cluster = new Cluster(new LocalNode("localhost", Map("nrv" -> 40373)), manager)

      val protocol = new NrvProtocol(cluster.localNode, 5000, 100)
      cluster.registerProtocol(protocol, default = true)

      bwl = new Bwl(definitions = List(definition), createQueue = queueFactory.factory, spnl = new Spnl)
      bwl.applySupport(responseTimeout = Some(2000))
      cluster.registerService(bwl)
      bwl.addMember(0, cluster.localNode)

      cluster.start()

      // Execute the test code
      test(this)
    } finally {
      cluster.stop()
      cluster = null
      bwl = null
      mockCallback = null
      queueFactory.after()
    }
  }

  def newTaskContext = new TaskContext(normalRate = 50, throttleRate = 50)

  trait Callback {
    def process(data: QueueTask.Data)
  }

  def okCallback(callback: Callback)(data: QueueTask.Data): Future[QueueTask.Result] = {
    import scala.concurrent.future
    import ExecutionContext.Implicits.global

    future {
      callback.process(data)
      QueueTask.Result.Ok
    }
  }
}

object BwlFixture {

  trait QueueFactory {
    type Factory = (Long, QueueDefinition, Service with QueueService) => Queue

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
      logDir.listFiles().foreach(_.delete())
      logDir.delete()
      logDir = null
    }
  }
}

trait SingleQueueFixture {
  this: BwlFixture =>

  lazy val definition = QueueDefinition("single", okCallback(mockCallback), newTaskContext)
}

trait MultipleQueueFixture {
  this: BwlFixture =>

  lazy val definition = QueueDefinition("weighted", okCallback(mockCallback), newTaskContext,
    priorities = List(Priority(1, weight = 33), Priority(2, weight = 66)))
}

@RunWith(classOf[JUnitRunner])
class TestBwl extends FunSuite {

  import BwlFixture._

  def singlePriorityQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - queue should enqueue and dequeue expected value") {
      new BwlFixture with SingleQueueFixture {}.runWithFixture((f) => {
        import ExecutionContext.Implicits.global

        val t = f.bwl.enqueue(0, f.definition.name, "hello")
        val id = Await.result(t, Duration.Inf)

        val expectedData = Map("token" -> "0", "id" -> id, "priority" -> 1, "data" -> "hello")
        verify(f.mockCallback, timeout(2000)).process(argEquals(expectedData))
        verifyNoMoreInteractions(f.mockCallback)
      })
    }

  }

  def multiplePrioritiesQueue(implicit queueFactory: QueueFactory) {

    test(queueFactory.name + " - enqueued priority weights should be respected") {
      import ExecutionContext.Implicits.global

      new BwlFixture with MultipleQueueFixture {}.runWithFixture((f) => {
        val enqueued = f.definition.priorities.flatMap(p => 1.to(200).map(i =>
          f.bwl.enqueue(i, f.definition.name, p.value, Some(p.value))))
        //  bwl.enqueue(i, double.name, Map("p" -> p.value, "i" -> i), Some(p.value))))
        Await.result(Future.sequence(enqueued), 5 seconds)

        val dataCaptor = ArgumentCaptor.forClass(classOf[Map[String, Any]])
        verify(f.mockCallback, timeout(5000).atLeast(100)).process(dataCaptor.capture())
        val values = dataCaptor.getAllValues.toList

        val p1Count = values.map(_("data")).count(_ == 1)
        val p2Count = values.map(_("data")).count(_ == 2)
        p2Count should be > p1Count
        println(s"1=$p1Count, 2=$p2Count")
      })
    }

  }

  testsFor(singlePriorityQueue(memoryQueueFactory))
  testsFor(multiplePrioritiesQueue(memoryQueueFactory))
  testsFor(singlePriorityQueue(persistentQueueFactory))
  testsFor(multiplePrioritiesQueue(persistentQueueFactory))
}
