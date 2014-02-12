package com.wajam.bwl.demo

import com.wajam.commons.Logging
import scala.concurrent.ExecutionContext
import com.wajam.nrv.cluster._
import com.wajam.nrv.protocol._
import com.wajam.nrv.extension.resource._
import com.wajam.nrv.protocol.codec.Codec
import com.wajam.spnl._
import com.wajam.bwl.{ ConsistentBwl, Bwl }
import com.wajam.bwl.queue.{ Priority, QueueDefinition }
import com.wajam.bwl.queue.log.LogQueue
import java.io.File
import com.wajam.nrv.service.{ Switchboard, ExplicitReplicaResolver, ActionSupportOptions, Service }
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.apache.log4j.PropertyConfigurator
import com.typesafe.config.ConfigFactory
import com.wajam.nrv.consistency.{ ConsistencyPersistence, ConsistencyMasterSlave }
import com.wajam.scn.client.{ ScnClientConfig, ScnClient }
import com.wajam.scn.{ Scn, ScnConfig }
import com.wajam.scn.storage.StorageType
import java.nio.file.Files
import com.wajam.nrv.extension.json.codec.JsonCodec
import java.util.concurrent.Executors

object Demo extends App with Logging {

  implicit private val executionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  PropertyConfigurator.configureAndWatch("etc/log4j.properties", 5000)
  log.info("Initializing Bwl movement")

  val config: DemoConfig = new DemoConfig(ConfigFactory.load())
  val server = new DemoServer(config)
  sys.addShutdownHook(server.stop())
  server.startAndBlock()
}

class DemoServer(config: DemoConfig)(implicit ec: ExecutionContext) extends Logging {
  val ports = Map("nrv" -> config.getProtocolNrvPort, "http" -> config.getProtocolHttpPort)
  val node = new LocalNode(config.getNodeListenAddress, ports)

  log.info("Local node is {}", node)

  lazy val zkClient = new ZookeeperClient(config.getZookeeperServers)

  val httpProtocol = createHttpProtocol()
  val nrvProtocol = createNrvProtocol()

  val (scnService, scnClient) = createScnServiceAndClient()

  val queueDefinitions = List(
    QueueDefinition("single", new DemoResource.DemoCallback("single", delay = 0L)),
    QueueDefinition("multiple", new DemoResource.DemoCallback("multiple", delay = 0L),
      priorities = Seq(Priority(1, weight = 3), Priority(2, weight = 1))))

  val bwlService = createBwlService(queueDefinitions)

  val demoService = createDemoService()

  val cluster = new Cluster(node, createClusterManager(), new ActionSupportOptions(), Some(nrvProtocol))

  cluster.registerProtocol(httpProtocol)
  cluster.registerProtocol(nrvProtocol)
  demoService.applySupport(protocol = Some(nrvProtocol), supportedProtocols = Some(Set(httpProtocol)))

  cluster.registerService(demoService)
  cluster.registerService(bwlService)
  cluster.registerService(scnService)

  bwlService.consistency.bindService(bwlService)

  def start(): Unit = {
    cluster.start()
    scnClient.start()
  }

  def startAndBlock(): Unit = {
    start()
    Thread.currentThread.join()
  }

  def stop(): Unit = {
    cluster.stop(config.getClusterShutdownTimeout)
  }

  private def createClusterManager(): ClusterManager = {
    config.getClusterManager match {
      case "static" => {
        val clusterManager = new StaticClusterManager
        clusterManager.addMembers(scnService, config.getScnClusterMembers)
        clusterManager.addMembers(bwlService, config.getBwlClusterMembers)
        clusterManager
      }
      case "zookeeper" => {
        new ZookeeperClusterManager(zkClient)
      }
    }
  }

  private def createNrvProtocol(): Protocol = {
    val baseProtocol = new NrvProtocol(
      node,
      config.getProtocolNrvConnectionTimeoutMs,
      config.getProtocolConnectionPoolMaxSize)

    val localProtocol = new NrvMemoryProtocol(
      baseProtocol.name,
      node)

    new NrvLocalBranchingProtocol(baseProtocol.name, node, localProtocol, baseProtocol)
  }

  private def createHttpProtocol(): HttpProtocol = {
    val httpProtocol = new HttpProtocol("http",
      node,
      config.getProtocolHttpConnectionTimeoutMs,
      config.getProtocolHttpConnectionPoolMaxSize)
    httpProtocol.registerCodec("text/plain", StringCodec)
    httpProtocol.registerCodec("application/json", new JsonCodec)
    httpProtocol
  }

  private def createScnServiceAndClient(): (Scn, ScnClient) = {
    val scnConfig = ScnConfig()
    val service = config.getClusterManager match {
      case "zookeeper" => new Scn("scn", scnConfig, StorageType.ZOOKEEPER, Some(zkClient))
      case _ => new Scn("scn", scnConfig, StorageType.MEMORY)
    }
    service.applySupport(switchboard = Some(new Switchboard("scn")))
    (service, new ScnClient(service, ScnClientConfig()))
  }

  private def createBwlService(definitions: Iterable[QueueDefinition]): Bwl with DemoBwlService = {

    val spnlPersistenceFactory = config.getClusterManager match {
      case "zookeeper" => new ZookeeperTaskPersistenceFactory(zkClient)
      case _ => new NoTaskPersistenceFactory
    }

    val queueFactory = new LogQueue.Factory(new File(config.getBwlPersistentQueueDirectory),
      config.getBwlPersistentQueueRolloverSize,
      config.getBwlPersistentQueueCommitFrequency,
      config.getBwlPersistentQueueCleanFrequency)

    val bwlSpnl = new Spnl()
    val service = new Bwl("bwl", definitions, queueFactory, ec, bwlSpnl, spnlPersistenceFactory) with ConsistentBwl with DemoBwlService {
      val spnl: Spnl = bwlSpnl
    }
    service.applySupport(responseTimeout = Some(config.getBwlTaskTimeout), nrvCodec = Some(StringCodec))

    if (config.getBwlConsistencyReplicationEnabled) {
      val consistencyLogDirectory = new File(config.getBwlConsistencyLogDirectory)
      Files.createDirectories(consistencyLogDirectory.toPath)

      val consistency = new ConsistencyMasterSlave(
        scnClient,
        new ConsistencyPersistence {
          def start() = {}
          def stop() = {}
          def explicitReplicasMapping = config.getBwlConsistencyExplicitReplicas
          def replicationLagSeconds(token: Long, node: Node) = None
          def updateReplicationLagSeconds(token: Long, node: Node, lag: Int) = {}
          def changeMasterServiceMember(token: Long, node: Node) = {}
        },
        config.getBwlConsistencyLogDirectory,
        txLogEnabled = true,
        replicationResolver = Some(new ExplicitReplicaResolver(config.getBwlConsistencyExplicitReplicas, service.resolver)))
      service.applySupport(consistency = Some(consistency))
    }

    service
  }

  private def createDemoService(): Service = {
    val service = new Service("demo") /*with CrtxBwlApi with CrtxNrvAPI with CrtxSpnlAPI with CrtxNrvConsistencyAPI*/ {
      protected val spnl = bwlService.spnl
      protected def queueViews(serviceName: String) = bwlService.queueViews(serviceName)
    }
    val demoResource = new DemoResource(bwlService, queueDefinitions)
    service.registerResources(demoResource)
    service
  }

  trait DemoBwlService {
    def spnl: Spnl
  }

  object StringCodec extends Codec {

    def encode(entity: Any, context: Any) = {
      (entity, context) match {
        case (value: String, charsetName: String) => value.getBytes(charsetName)
        case (value: String, _) => value.getBytes
        case (null, _) => Array[Byte]()
      }
    }

    def decode(data: Array[Byte], context: Any) = {
      if (data.length > 0) {
        context match {
          case charsetName: String => new String(data, charsetName)
          case null => new String(data)
        }
      } else {
        ""
      }

    }
  }
}