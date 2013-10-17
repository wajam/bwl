package com.wajam.bwl.queue.log

import com.wajam.bwl.queue._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.data.MValue._
import java.io.File
import java.nio.file.{ Files, Paths }
import com.wajam.nrv.consistency.{ ConsistencyMasterSlave, ResolvedServiceMember, TransactionRecorder }
import com.wajam.nrv.service.Service
import com.wajam.nrv.consistency.log.{ TimestampedRecord, FileTransactionLog }
import com.wajam.nrv.consistency.replication.TransactionLogReplicationIterator
import com.wajam.bwl.QueueResource._
import com.wajam.bwl.queue.Priority
import com.wajam.bwl.queue.QueueDefinition
import java.util.concurrent.atomic.AtomicInteger
import LogQueue._

/**
 * Persistent queue using NRV transaction log as backing storage. Each priority is appended to separate log.
 */
class LogQueue(val token: Long, service: Service, val definition: QueueDefinition,
               recorderFactory: RecorderFactory) extends Queue {

  private val recorders: Map[Int, TransactionRecorder] = priorities.map(p => p.value -> recorderFactory(token, definition, p)).toMap
  private var rebuildEndPositions: Map[Int, Timestamp] = recorders.collect {
    case (priority, recorder) if recorder.currentConsistentTimestamp.isDefined => priority -> recorder.currentConsistentTimestamp.get
  }.toMap

  private val totalTaskCount = new AtomicInteger()

  def enqueue(taskItem: QueueItem.Task) = {
    recorders.get(taskItem.priority) match {
      case Some(recorder) => {
        val request = item2request(taskItem)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))

        // TODO: Explain WTF is this!!!
        val minIncrementId = synchronized {
          rebuildEndPositions.get(taskItem.priority) match {
            case Some(id) => id
            case None => {
              rebuildEndPositions += taskItem.priority -> taskItem.taskId
              taskItem.taskId
            }
          }
        }
        if (taskItem.taskId > minIncrementId) {
          totalTaskCount.incrementAndGet()
        }
      }
      case None => // TODO
    }
    taskItem
  }

  def ack(ackItem: QueueItem.Ack) = {
    feeder.pendingTaskPriorityFor(ackItem.taskId).flatMap(priority => recorders.get(priority)) match {
      case Some(recorder) => {
        val request = item2request(ackItem)
        recorder.appendMessage(request)
        recorder.appendMessage(createSyntheticSuccessResponse(request))
        totalTaskCount.decrementAndGet()
      }
      case None => // TODO:
    }
    ackItem
  }

  val feeder = new LogQueueFeeder(definition, createPriorityQueueReader)

  def stats: QueueStats = LogQueueStats

  /**
   * Creates a new LogQueueFeeder.QueueReader. This method is passed as a factory function to the
   * LogQueueFeeder constructor.
   */
  private def createPriorityQueueReader(priority: Int, startTimestamp: Option[Timestamp]): LogQueueReader = {
    val recorder = recorders(priority)

    def createLogQueueReader: LogQueueReader = {
      val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
      val initialTimestamp = startTimestamp.getOrElse(findStartTimestamp(txLog))

      // Rebuild in-memory states by reading all the persisted tasks from the transaction log once
      val (total, processed) = rebuildPriorityQueueState(priority, initialTimestamp)
      totalTaskCount.addAndGet(total)

      val itr = new TransactionLogReplicationIterator(recorder.member,
        initialTimestamp, txLog, recorder.currentConsistentTimestamp)
      LogQueueReader(service, itr, processed)
    }

    new DelayedQueueReader(recorder, createLogQueueReader)
  }

  /**
   * Reads all the persisted tasks from the transaction logs and computes the number of unprocessed tasks
   * (i.e. excluding acknowledged tasks) and keep a list of processed tasks (i.e. the acknowledged ones).
   * The processed tasks will be skip when read again from the feeder.
   */
  private def rebuildPriorityQueueState(priority: Int, initialTimestamp: Timestamp): (Int, Set[Timestamp]) = {

    import com.wajam.commons.Closable.using

    val recorder = recorders(priority)
    val txLog = recorder.txLog.asInstanceOf[FileTransactionLog]
    val member = recorder.member
    val endTimestamp = synchronized(rebuildEndPositions(priority))

    using(new TransactionLogReplicationIterator(member, initialTimestamp, txLog, recorder.currentConsistentTimestamp)) { itr =>
      val items = for {
        msgOpt <- itr
        msg <- msgOpt
        item <- message2item(msg, service)
      } yield item

      var all = Set[Timestamp]()
      var processed = Set[Timestamp]()

      import scala.util.control.Breaks._
      breakable {
        items.takeWhile(_.timestamp <= endTimestamp).foreach { item =>
          item match {
            case taskItem: QueueItem.Task => all += taskItem.taskId
            case ackItem: QueueItem.Ack if all.contains(ackItem.taskId) => {
              all -= ackItem.taskId
              processed += ackItem.taskId
            }
            case _ => // Ignore other items. They are either ack for tasks prior the initial timestamp or responses
          }

          if (item.timestamp == endTimestamp) {
            break()
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
    def totalTasks = totalTaskCount.get()

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
  def create(dataDir: File, logFileRolloverSize: Int = 52428800, logCommitFrequency: Int = 2000)(token: Long, definition: QueueDefinition, service: Service): Queue = {

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

  private[log] def message2item(message: Message, service: Service): Option[QueueItem] = {
    import com.wajam.nrv.extension.resource.ParamsAccessor._

    message.function match {
      case MessageType.FUNCTION_CALL if message.path == "/enqueue" => {
        message.timestamp.map(QueueItem.Task(_, message.token, message.param[Int](TaskPriority), message.getData[Any]))
      }
      case MessageType.FUNCTION_CALL if message.path == "/ack" => message.timestamp.map(QueueItem.Ack(_, message.param[Long](TaskId)))
      case _ => throw new IllegalStateException(s"Unsupported message path: ${message.path}")
    }
  }

  private[log] def item2request(item: QueueItem): InMessage = {
    item match {
      case taskItem: QueueItem.Task => {
        val params = Iterable[(String, MValue)](TaskPriority -> taskItem.priority)
        createSyntheticRequest(taskItem.taskId, taskItem.token, "/enqueue", params, taskItem.data)
      }
      case ackItem: QueueItem.Ack => createSyntheticRequest(ackItem.ackId, -1, "/ack")
    }
  }

  private[log] def createSyntheticRequest(taskId: Timestamp, taskToken: Long, path: String,
                                          params: Iterable[(String, MValue)] = Nil, data: Any = null): InMessage = {
    val extraParams = Iterable[(String, MValue)](TaskId -> taskId.value, TaskToken -> taskToken)
    val request = new InMessage(params ++ extraParams, data = data)
    request.token = taskToken
    request.timestamp = Some(taskId)
    request.function = MessageType.FUNCTION_CALL
    request.path = path
    request
  }

  private[log] def createSyntheticSuccessResponse(request: InMessage): OutMessage = {
    val response = new OutMessage(code = 200)
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response
  }
}
