package com.wajam.bwl

import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.cluster.{ Node, LocalNode, StaticClusterManager, Cluster }
import com.wajam.bwl.queue.{ Priority, QueueTask, Queue, QueueDefinition }
import com.wajam.nrv.protocol.NrvProtocol
import com.wajam.spnl.{ TaskContext, Spnl }
import scala.util.Random
import com.wajam.nrv.service.{ TokenRange, ServiceMember, Service }
import com.wajam.bwl.queue.memory.MemoryQueue
import java.io.File
import java.nio.file.Files
import com.wajam.bwl.queue.log.LogQueue
import org.apache.commons.io.FileUtils
import org.mockito.Mockito._
import scala.concurrent.ExecutionContext
import com.wajam.bwl.BwlFixture.QueueFactory

trait BwlFixture extends CallbackFixture with MockitoSugar {

  import BwlFixture._

  var bwl: Bwl = null
  var cluster: Cluster = null

  val localNode = new LocalNode("localhost", Map("nrv" -> 40373))
  val localMember: ServiceMember = new ServiceMember(TokenRange.MaxToken / 2, localNode)
  val remoteMember: ServiceMember = new ServiceMember(TokenRange.MaxToken, new Node("localhost", Map("nrv" -> 54321)))

  def definitions: Seq[QueueDefinition]

  def createBwlService(queueFactory: QueueFactory)(implicit random: Random = Random) = {
    new Bwl("bwl", definitions, queueFactory.factory, new Spnl)
  }

  def runWithFixture(test: (BwlFixture) => Any)(implicit queueFactory: QueueFactory, random: Random = Random) {
    try {
      queueFactory.before()

      val manager = new StaticClusterManager
      cluster = new Cluster(localNode, manager)

      val protocol = new NrvProtocol(cluster.localNode, 5000, 100)
      cluster.registerProtocol(protocol, default = true)

      bwl = createBwlService(queueFactory)
      bwl.applySupport(responseTimeout = Some(3000))
      cluster.registerService(bwl)
      bwl.addMember(localMember)
      bwl.addMember(remoteMember)

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

trait ConsistentBwlFixture extends BwlFixture {

  def consistentBwl: ConsistentBwl = bwl.asInstanceOf[ConsistentBwl]

  override def createBwlService(queueFactory: QueueFactory)(implicit random: Random = Random) = {
    new Bwl("consistent-bwl", definitions, queueFactory.factory, new Spnl) with ConsistentBwl
  }

  def runWithConsistentFixture(test: (ConsistentBwlFixture) => Any)(implicit queueFactory: QueueFactory, random: Random = Random) {
    runWithFixture((f) => {
      test(f.asInstanceOf[ConsistentBwlFixture])
    })
  }
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

  @volatile
  var callbackCallCount: Int = 0

  def taskCallback: QueueTask.Callback
}

abstract class OkCallbackFixture(delay: Long = 0L) extends CallbackFixture {

  import scala.concurrent.future
  import ExecutionContext.Implicits.global

  def taskCallback = (data) => future {
    callbackCallCount += 1
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
    callbackCallCount += 1
    Thread.sleep(delay)
    mockCallback.process(data)
    QueueTask.Result.Fail(new Exception(), ignore)
  }
}

trait SinglePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val singlePriorityDefinition = QueueDefinition("single", taskCallback, newTaskContext)
  def definitions = List(singlePriorityDefinition)
}

trait MultiplePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val multiplePriorityDefinition = QueueDefinition("multiple", taskCallback, newTaskContext,
    priorities = List(Priority(1, weight = 66), Priority(2, weight = 33)))
  def definitions = List(multiplePriorityDefinition)
}

trait MultipleQueuesFixture extends SinglePriorityQueueFixture with MultiplePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  override def definitions = List(singlePriorityDefinition, multiplePriorityDefinition)
}

