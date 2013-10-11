package com.wajam.bwl.queue.log

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.data.MValue._
import java.io.File
import java.nio.file.{ Files, Paths }
import com.wajam.nrv.consistency.{ ConsistencyMasterSlave, ResolvedServiceMember, TransactionRecorder }
import com.wajam.bwl.queue.log.LogQueue.RecorderFactory
import com.wajam.nrv.service.Service
import com.wajam.nrv.consistency.log.{ TimestampedRecord, FileTransactionLog }
import com.wajam.nrv.consistency.replication.TransactionLogReplicationIterator
import scala.collection.mutable
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition

/**
 * Persistent queue using NRV transaction log as backing storage. Each priority is appended to separate log.
 */
class LogQueue(val token: Long, service: Service with QueueService, val definition: QueueDefinition,
               recorderFactory: RecorderFactory) extends Queue {

  private val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition, p)).toMap

  private var totalTaskCount = 0

  def enqueue(taskItem: QueueItem.Task) {
    recorders.get(taskItem.priority) match {
      case Some(recorder) => {
        val params = Iterable[(String, MValue)](TaskPriority -> taskItem.priority)
        val request = createSyntheticRequest(taskItem.taskId, taskItem.token, "/enqueue", params, taskItem.data)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
      }
      case None => // TODO
    }
  }

  def ack(ackItem: QueueItem.Ack) {
    feeder.pendingTaskPriorityFor(ackItem.taskId).flatMap(priority => recorders.get(priority)) match {
      case Some(recorder) => {
        val request = createSyntheticRequest(ackItem.ackId, -1, "/ack")
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
      }
      case None => // TODO:
    }
  }

  val feeder = new LogQueueFeeder(definition, createPriorityQueueReader)

  def stats: QueueStats = LogQueueStats

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
  private def createPriorityQueueReader(priority: Int, startTimestamp: Option[Timestamp]): LogQueueReader = {
    val recorder = recorders(priority)

    def createLogQueueReader: LogQueueReader = {
      val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
      val initialTimestamp = startTimestamp.getOrElse(findStartTimestamp(txLog))

      // TODO: rebuild in-memory priority states
      val (total, processed) = rebuildPriorityQueueState(priority, initialTimestamp)
      totalTaskCount += total

      val itr = new TransactionLogReplicationIterator(recorder.member,
        initialTimestamp, txLog, recorder.currentConsistentTimestamp)
      LogQueueReader(service, itr, processed)
    }

    new DelayedQueueReader(recorder, createLogQueueReader)
  }

  private def rebuildPriorityQueueState(priority: Int, initialTimestamp: Timestamp): (Int, mutable.Set[Timestamp]) = {

    import com.wajam.commons.Closable.using
    import LogQueue.message2item

    val recorder = recorders(priority)
    val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
    val member = recorder.member

    using(new TransactionLogReplicationIterator(member, initialTimestamp, txLog, Some(Long.MaxValue))) { itr =>
      val items = for (msgOpt <- itr; msg <- msgOpt; item <- message2item(msg, service)) yield item

      // TODO: do not read beyond max timestamp which is the oldest item appended after the queue start
      var all = mutable.Set[Timestamp]()
      var processed = mutable.Set[Timestamp]()
      items.foreach {
        case item: QueueItem.Task => all += item.taskId
        case item: QueueItem.Ack => {
          if (all.remove(item.taskId)) {
            processed += item.taskId
          }
        }
      }

      (all.size, processed)
    }
  }

  /**
   * Returns the oldest log record timestamp starting the beginning of the transaction log. The records may be written
   * out of order in the log. So this methods reads more than the initial record returns the oldest timestamp.
   */
  def findStartTimestamp(txLog: FileTransactionLog): Timestamp = {
    import com.wajam.commons.Closable.using
    using(txLog.read) { itr =>
      val firstTimestamp = txLog.firstRecord(timestamp = None).map(_.timestamp)
      val startRange = itr.takeWhile(_.consistentTimestamp < firstTimestamp).collect {
        case r: TimestampedRecord => r.timestamp.value
      }.min
      startRange
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
  private class DelayedQueueReader(recorder: TransactionRecorder, createReader: => LogQueueReader)
      extends LogQueueReader {

    private var reader: Option[LogQueueReader] = None

    private def getOrCreateReader: LogQueueReader = reader match {
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

    def delayedTasks = getOrCreateReader.delayedTasks
  }

  private object InfiniteEmptyReader extends LogQueueReader {
    def hasNext = true

    def next() = None

    def close() {}

    def delayedTasks = Nil
  }

  private object LogQueueStats extends QueueStats {
    def totalTasks = totalTaskCount

    def pendingTasks = feeder.pendingTasks.toIterable

    def delayedTasks = feeder.delayedTasks.toIterable
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

  def message2item(message: Message, service: QueueService with Service): Option[QueueItem] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._

    message.function match {
      case MessageType.FUNCTION_CALL if message.path == "/enqueue" => {
        message.timestamp.map(QueueItem.Task(_, message.token, message.param[Int](TaskPriority), message.getData[Any]))
      }
      case MessageType.FUNCTION_CALL if message.path == "/ack" => message.timestamp.map(QueueItem.Ack(_, message.param[Long](TaskId)))
      case _ => None
    }
  }
}
