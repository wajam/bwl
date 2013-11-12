package com.wajam.bwl.demo

import com.typesafe.config.Config
import scala.collection.JavaConversions._
import com.wajam.nrv.cluster.Node

class DemoConfig(config: Config) {
  def getNodeListenAddress: String = config.getString("nrv.node.listen-address")
  def getClusterManager: String = config.getString("nrv.cluster.manager")
  def getClusterShutdownTimeout: Int = config.getInt("nrv.cluster.shutdown-timeout")
  def getZookeeperServers: String = config.getString("nrv.zookeeper.servers")

  def getProtocolNrvPort: Int = config.getInt("nrv.protocol.nrv.port")
  def getProtocolNrvConnectionTimeoutMs: Int = config.getInt("nrv.protocol.nrv.connection.timeout-ms")
  def getProtocolConnectionPoolMaxSize: Int = config.getInt("nrv.protocol.nrv.connection.pool.max-size")

  def getProtocolHttpPort: Int = config.getInt("nrv.protocol.http.port")
  def getProtocolHttpConnectionTimeoutMs: Int = config.getInt("nrv.protocol.http.connection.timeout-ms")
  def getProtocolHttpConnectionPoolMaxSize: Int = config.getInt("nrv.protocol.http.connection.pool.max-size")

  def getScnClusterMembers: List[String] = config.getStringList("scn.cluster.members").toList

  def getBwlClusterMembers: List[String] = config.getStringList("bwl.cluster.members").toList
  def getBwlTaskTimeout: Long = config.getLong("bwl.task.timeout")
  def getBwlPersistentQueueDirectory: String = config.getString("bwl.persist-queue.directory")
  def getBwlPersistentQueueRolloverSize: Int = config.getInt("bwl.persist-queue.rollover-size-bytes")
  def getBwlPersistentQueueCommitFrequency: Int = config.getInt("bwl.persist-queue.commit-frequency-ms")
  def getBwlPersistentQueueCleanFrequency: Int = config.getInt("bwl.persist-queue.clean-frequency-ms")

  def getBwlConsistencyReplicationEnabled: Boolean = config.getBoolean("bwl.consistency.replication-enabled")
  def getBwlConsistencyLogDirectory: String = config.getString("bwl.consistency.log-directory")
  def getBwlConsistencyExplicitReplicas: Map[Long, List[Node]] = {
    def nodeFromStringNoProtocol(hostnamePort: String): Node = {
      val Array(host, port) = hostnamePort.split(":")
      new Node(host, Map("nrv" -> port.toInt))
    }

    val replicasConfig: Config = config.getConfig("bwl.consistency.replicas")
    replicasConfig.entrySet().map { entry =>
      val key = entry.getKey.replaceAll("\"", "")
      val token = key.toLong
      val nodes = replicasConfig.getStringList(key).map(nodeFromStringNoProtocol).toList
      (token, nodes)
    }.toMap
  }
}
