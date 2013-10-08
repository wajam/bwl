package com.wajam.bwl.queue.log

import com.wajam.bwl.queue.{ QueueService, Queue }
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.data.MValue._
import java.io.File
import java.nio.file.{ Files, Paths }
import com.wajam.nrv.consistency.{ ConsistencyMasterSlave, ResolvedServiceMember, TransactionRecorder }
import com.wajam.bwl.queue.log.LogQueue.RecorderFactory
import com.wajam.nrv.service.Service
import com.wajam.nrv.consistency.log.{ TimestampedRecord, FileTransactionLog }
import com.wajam.bwl.queue.log.LogQueueFeeder.QueueReader
import com.wajam.nrv.consistency.replication.TransactionLogReplicationIterator
import scala.collection.mutable
import com.wajam.commons.Closable
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition

/**
 * Persistent queue using NRV transaction log as backing storage. Each priority is appended to separate log.
 */
class LogQueue(val token: Long, service: Service with QueueService, val definition: QueueDefinition,
               recorderFactory: RecorderFactory) extends Queue {

  val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition, p)).toMap
  val reloadedAck: mutable.Set[Timestamp] = mutable.Set()

  def enqueue(taskId: Timestamp, taskToken: Long, taskPriority: Int, taskData: Any) {
    recorders.get(taskPriority) match {
      case Some(recorder) => {
        val params = Iterable[(String, MValue)](TaskPriority -> taskPriority)
        val request = createSyntheticRequest(taskId, taskToken, "/enqueue", params, taskData)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
      }
      case None => // TODO
    }
  }

  def ack(ackId: Timestamp, taskId: Timestamp) {
    feeder.pendingEntry(taskId).flatMap(entry => recorders.get(entry.priority)) match {
      case Some(recorder) => {
        val request = createSyntheticRequest(ackId, -1, "/ack")
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
      }
      case None => // TODO:
    }
  }

  val feeder = new LogQueueFeeder(definition, createPriorityQueueReader)

  private def createSyntheticRequest(taskId: Timestamp, taskToken: Long, path: String,
                                     params: Iterable[(String, MValue)] = Nil, data: Any = null): InMessage = {
    val extraParams = Iterable[(String, MValue)](TaskId -> taskId.value, TaskToken -> taskToken)
    val request = new InMessage(params ++ extraParams, data = data)
    request.token = taskToken
    request.timestamp = Some(taskId)
    request.function = MessageType.FUNCTION_CALL
    request.path = path
    request
  }

  private def createSyntheticSuccessResponse(request: InMessage): OutMessage = {
    val response = new OutMessage(code = 200)
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response
  }

  /**
   * Creates a new LogQueueFeeder.QueueReader. This method is passed as a factory function to the
   * LogQueueFeeder constructor.
   */
  private def createPriorityQueueReader(priority: Int, startTimestamp: Option[Timestamp]): QueueReader = {
    val recorder = recorders(priority)

    def createLogQueueReader: QueueReader = {
      // TODO: rebuild in-memory priority states including 'reloadedAck'
      val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
      val itr = new TransactionLogReplicationIterator(recorder.member,
        startTimestamp.getOrElse(findStartTimestamp(txLog)), txLog, recorder.currentConsistentTimestamp)
      new LogQueueReader(service, itr, reloadedAck)
    }

    new DelayedQueueReader(recorder, createLogQueueReader)
  }

  /**
   * Returns the oldest log record timestamp starting the beginning of the transaction log. The records may be written
   * out of order in the log. So this methods reads more than the initial record returns the oldest timestamp.
   */
  def findStartTimestamp(txLog: FileTransactionLog): Timestamp = {
    val itr = txLog.read
    try {
      val firstTimestamp = txLog.firstRecord(timestamp = None).map(_.timestamp)
      val startRange = itr.takeWhile(_.consistentTimestamp < firstTimestamp).collect {
        case r: TimestampedRecord => r
      }.toVector.sortBy(_.timestamp)
      startRange.head.timestamp
    } finally {
      itr.close()
    }
  }

  def start() {
    recorders.valuesIterator.foreach(_.start())
  }

  def stop() {
    recorders.valuesIterator.foreach(_.kill())
  }

  /**
   * Queue reader wrapper which ensure that log exists and is NOT empty before creating the real LogQueueReader.
   * Achieved by waiting until TransactionRecorder produces a valid consistent timestamp.
   */
  private class DelayedQueueReader(recorder: TransactionRecorder, createReader: => QueueReader)
      extends Iterator[Option[QueueEntry.Enqueue]] with Closable {

    private var reader: Option[QueueReader] = None

    private def getOrCreateReader: QueueReader = reader match {
      case None if recorder.currentConsistentTimestamp.isEmpty => InfiniteEmptyReader
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

  private object InfiniteEmptyReader extends Iterator[Option[QueueEntry.Enqueue]] with Closable {
    def hasNext = true

    def next() = None

    def close() {}
  }

}

object LogQueue {

  type RecorderFactory = (Long, QueueDefinition, Priority) => TransactionRecorder

  /**
   * Creates a new LogQueue. This factory method is usable as [[com.wajam.bwl.queue.Queue.QueueFactory]] with the
   * Bwl service
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

    def createRecorder(token: Long, definition: QueueDefinition, priority: Priority): TransactionRecorder = {
      Files.createDirectories(logDir)
      val name = s"${definition.name}#${priority.value}"
      val txLog = new FileTransactionLog(name, token, logDir.toString, logFileRolloverSize)
      new TransactionRecorder(ResolvedServiceMember(service, token), txLog, consistencyDelay,
        service.responseTimeout, logCommitFrequency, {})
    }

    new LogQueue(token, service, definition, createRecorder)
  }
}
