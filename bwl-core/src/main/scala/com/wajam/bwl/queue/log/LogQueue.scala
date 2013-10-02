package com.wajam.bwl.queue.log

import com.wajam.bwl.queue.{ QueueService, QueueDefinition, Queue }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{ MessageType, OutMessage, InMessage }
import java.io.File
import java.nio.file.{Files, Paths}
import com.wajam.nrv.consistency.{ ConsistencyMasterSlave, ResolvedServiceMember, TransactionRecorder }
import com.wajam.bwl.queue.log.LogQueue.{ QueueReader, RecorderFactory }
import com.wajam.nrv.service.Service
import com.wajam.nrv.consistency.log.FileTransactionLog
import com.wajam.bwl.queue.log.LogQueueFeeder.PriorityReader
import com.wajam.nrv.consistency.replication.TransactionLogReplicationIterator
import com.wajam.bwl.utils.ClosablePeekIterator
import scala.collection.mutable
import com.wajam.nrv.utils.Closable

class LogQueue(val token: Long, service: Service with QueueService, val definition: QueueDefinition,
               recorderFactory: RecorderFactory) extends Queue {

  val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition)).toMap
  val reloadedAck: mutable.Set[Timestamp] = mutable.Set()

  def enqueue(taskMsg: InMessage, priority: Int) {
    recorders.get(priority) match {
      case Some(recorder) => {
        recorder.appendMessage(taskMsg)
        recorder.appendMessage(createSyntheticSuccessResponse(taskMsg))
      }
      case None => // TODO
    }
  }

  def ack(id: Timestamp, ackMessage: InMessage) {
    feeder.pendingEntry(id).flatMap(entry => recorders.get(entry.priority)) match {
      case Some(recorder) => {
        recorder.appendMessage(ackMessage)
        recorder.appendMessage(createSyntheticSuccessResponse(ackMessage))
      }
      case None => // TODO:
    }
  }

  private def createSyntheticSuccessResponse(request: InMessage): OutMessage = {
    val response = new OutMessage()
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response.code = 200
    response
  }

  val feeder = new LogQueueFeeder(definition, createPriorityQueueFeeder)

  private def createPriorityQueueFeeder(priority: Int, startTimestamp: Option[Timestamp]): PriorityReader = {
    val recorder = recorders(priority)

    def createLogQueueReader: QueueReader = {
      // TODO: support missing start timestamp
      // TODO: rebuild in-memory priority states including 'reloadedAck'
      val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
      val itr = new TransactionLogReplicationIterator(recorder.member, startTimestamp.get, txLog,
        recorder.currentConsistentTimestamp)
      new LogQueueReader(service, itr, reloadedAck)
    }

    new ClosablePeekIterator(new DelayedQueueReader(recorder, createLogQueueReader))
  }

  def start() {
    recorders.valuesIterator.foreach(_.start())
  }

  def stop() {
    recorders.valuesIterator.foreach(_.kill())
  }

  /**
   * Queue reader wrapper which ensure that log exists and is NOT empty before creating the real LogQueueReader.
   * Achieved by waiting until the recorder produce a valid consistent timestamp.
   */
  class DelayedQueueReader(recorder: TransactionRecorder, createReader: => QueueReader)
    extends Iterator[Option[QueueEntry.Enqueue]] with Closable {

    private var reader: Option[QueueReader] = None

    private def getOrCreateReader: QueueReader = reader match {
      case None if recorder.currentConsistentTimestamp.isEmpty => EmptyUnclosableReader
      case None => {
        val itr = createReader
        reader = Some(itr)
        itr
      }
      case Some(itr) => itr
    }

    def hasNext = getOrCreateReader.hasNext

    def next() = getOrCreateReader.next()

    def close() {
      reader.foreach(_.close())
    }
  }

  object EmptyUnclosableReader extends Iterator[Option[QueueEntry.Enqueue]] with Closable {
    def hasNext = true
    def next() = None
    def close() {}
  }
}

object LogQueue {

  type QueueReader = Iterator[Option[QueueEntry.Enqueue]] with Closable

  type RecorderFactory = (Long, QueueDefinition) => TransactionRecorder

  /**
   * LogQueue factory method usable as [[com.wajam.bwl.queue.Queue.QueueFactory]] with Bwl service
   */
  def create(dataDir: File, logFileRolloverSize: Int = 52428800, logCommitFrequency: Int = 2000)(token: Long, definition: QueueDefinition, service: Service with QueueService): Queue = {

    val logDir = Paths.get(dataDir.getCanonicalPath, service.name, "queues")
    def consistencyDelay: Long = {
      val timestampTimeout = service.consistency match {
        case consistency: ConsistencyMasterSlave => consistency.timestampGenerator.responseTimeout
        case _ => 0
      }
      timestampTimeout + 250
    }

    def createRecorder(token: Long, definition: QueueDefinition): TransactionRecorder = {
      Files.createDirectories(logDir)
      val txLog = new FileTransactionLog(definition.name, token, logDir.toString, logFileRolloverSize)
      new TransactionRecorder(ResolvedServiceMember(service, token), txLog, consistencyDelay,
        service.responseTimeout, logCommitFrequency, {})
    }

    new LogQueue(token, service, definition, createRecorder)
  }
}
