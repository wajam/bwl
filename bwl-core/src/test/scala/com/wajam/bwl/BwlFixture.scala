package com.wajam.bwl

import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.cluster.{ Node, LocalNode, StaticClusterManager, Cluster }
import com.wajam.bwl.queue._
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
import scala.concurrent.{ Future, ExecutionContext }
import com.wajam.bwl.BwlFixture.FixtureQueueFactory
import org.mockito.Matchers._
import com.wajam.bwl.queue.QueueDefinition
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.wajam.tracing.{ TraceRecorder, LoggingTraceRecorder, Tracer }

trait BwlFixture extends CallbackFixture with MockitoSugar {

  import BwlFixture._

  var bwl: Bwl = null
  var cluster: Cluster = null
  val mockTraceRecorder: TraceRecorder = mock[TraceRecorder]

  val localNode = new LocalNode("localhost", Map("nrv" -> 40373))
  val localMember: ServiceMember = new ServiceMember(TokenRange.MaxToken / 2, localNode)
  val remoteMember: ServiceMember = new ServiceMember(TokenRange.MaxToken, new Node("localhost", Map("nrv" -> 54321)))

  def definitions: Seq[QueueDefinition]

  def createBwlService(queueFactory: FixtureQueueFactory)(implicit random: Random = Random) = {
    new Bwl("bwl", definitions, queueFactory.factory, ExecutionContext.global, new Spnl)
  }

  def runWithFixture(test: (BwlFixture) => Any)(implicit queueFactory: FixtureQueueFactory, random: Random = Random) {
    try {
      queueFactory.before()

      val manager = new StaticClusterManager
      cluster = new Cluster(localNode, manager)
      cluster.applySupport(tracer = Some(new Tracer(mockTraceRecorder)))

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
      reset(mockTraceRecorder)
      queueFactory.after()
    }
  }

  def newTaskContext = new TaskContext(normalRate = 50, throttleRate = 50)
}

trait ConsistentBwlFixture extends BwlFixture {

  def consistentBwl: ConsistentBwl = bwl.asInstanceOf[ConsistentBwl]

  override def createBwlService(queueFactory: FixtureQueueFactory)(implicit random: Random = Random) = {
    new Bwl("consistent-bwl", definitions, queueFactory.factory, ExecutionContext.global, new Spnl) with ConsistentBwl
  }

  def runWithConsistentFixture(test: (ConsistentBwlFixture) => Any)(implicit queueFactory: FixtureQueueFactory, random: Random = Random) {
    runWithFixture((f) => {
      test(f.asInstanceOf[ConsistentBwlFixture])
    })
  }
}

object BwlFixture {

  trait FixtureQueueFactory {
    implicit val random = new Random(999)

    def name: String

    def before() = {}

    def after() = {}

    def factory: QueueFactory
  }

  def memoryQueueFactory = new FixtureQueueFactory {
    val name = "memory"
    val factory: QueueFactory = new MemoryQueue.Factory
  }

  def persistentQueueFactory = new FixtureQueueFactory {
    var logDir: File = null
    var factory: QueueFactory = null

    val name = "persistent"

    override def before() = {
      logDir = Files.createTempDirectory("TestBwl").toFile
      factory = new LogQueue.Factory(logDir)
    }

    override def after() = {
      FileUtils.deleteDirectory(logDir)
      logDir = null
    }
  }

  class SpyQueueFactory(queueFactory: FixtureQueueFactory) extends FixtureQueueFactory {

    private var spyQueues: List[Queue] = Nil

    def allQueues = spyQueues

    def name = queueFactory.name

    override def before() = queueFactory.before()

    override def after() = queueFactory.after()

    def factory = new QueueFactory {
      def createQueue(token: Long, definition: QueueDefinition, service: Service, instrumented: Boolean = false) = {
        val queue = queueFactory.factory.createQueue(token, definition, service, instrumented)
        spyQueues = spy[Queue](queue) :: spyQueues
        spyQueues.head
      }
    }
  }

}

trait CallbackFixture extends MockitoSugar {

  val mockCallback: QueueCallback = mock[QueueCallback]

  def result: QueueCallback.Result

  def delay: Long

  when(mockCallback.execute(anyObject())(anyObject())).thenAnswer(new Answer[Future[QueueCallback.Result]] {
    import scala.concurrent.future

    def answer(iom: InvocationOnMock) = {
      val args = iom.getArguments
      implicit val ec = args(1).asInstanceOf[ExecutionContext]
      future {
        Tracer.currentTracer.get.time("****: CallbackFixture.execute") {
          Thread.sleep(delay)
          result
        }
      }
    }
  })
}

abstract class OkCallbackFixture(val delay: Long = 0L) extends CallbackFixture {
  def result = QueueCallback.Result.Ok
}

abstract class ErrorCallbackFixture(resultError: QueueCallback.ResultError, useResultException: Boolean = false,
                                    val delay: Long = 0L) extends CallbackFixture {
  def result = {
    if (useResultException) {
      throw new QueueCallback.ResultException("", resultError)
    }
    resultError
  }
}

trait SinglePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val singlePriorityDefinition = QueueDefinition("single", mockCallback, newTaskContext)

  def definitions = List(singlePriorityDefinition)
}

trait MultiplePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  lazy val multiplePriorityDefinition = QueueDefinition("multiple", mockCallback, newTaskContext,
    priorities = List(Priority(1, weight = 66), Priority(2, weight = 33)))

  def definitions = List(multiplePriorityDefinition)
}

trait MultipleQueuesFixture extends SinglePriorityQueueFixture with MultiplePriorityQueueFixture {
  this: BwlFixture with CallbackFixture =>

  override def definitions = List(singlePriorityDefinition, multiplePriorityDefinition)
}

