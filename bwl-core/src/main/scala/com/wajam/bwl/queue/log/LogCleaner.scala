package com.wajam.bwl.queue.log

import com.wajam.nrv.consistency.log.FileTransactionLog
import com.wajam.nrv.utils.timestamp.Timestamp
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random
import com.wajam.commons.{ CurrentTime, Logging }
import scala.concurrent.ExecutionContext

/**
 * Job to clean up transaction log older than the specified `oldestTimestamp`. The job is never triggered more than
 * once during a `cleanFrequencyInMs` time period. The job is lazy and the method `tick()` must be invoked from time to
 * time to trigger the cleanup job. The triggered cleanup job is run asynchronously and does not block the `tick`
 * method.
 */
class LogCleaner(txLog: FileTransactionLog, oldestTimestamp: => Option[Timestamp],
                 cleanFrequencyInMs: Long)(implicit ec: ExecutionContext, random: Random = Random) extends CurrentTime with Logging {

  @volatile
  private var nextCleanupTime: Long = currentTime + cleanFrequencyInMs / 2 +
    (math.abs(random.nextLong()) % cleanFrequencyInMs / 2)

  private val running = new AtomicBoolean(false)

  /**
   * Trigger the cleanup job if the job scheduled time has come. Returns a true if the cleanup job has been
   * triggered by this call or None if not the case.
   */
  def tick(): Boolean = {
    val now = currentTime
    if (now >= nextCleanupTime && running.compareAndSet(false, true)) {
      nextCleanupTime = now + cleanFrequencyInMs

      debug(s"Cleaning up '${txLog.service}' extra log files")

      import scala.concurrent._
      val cleanupFuture = future(cleanupLogFiles())

      cleanupFuture onFailure {
        case error => {
          warn(s"Cleanup error '${txLog.service}': ", error)
          running.set(false)
        }
      }
      cleanupFuture onSuccess {
        case deletedFiles => {
          debug(s"Cleanup '${txLog.service}' deleted ${deletedFiles.size} log file(s).")
          running.set(false)
        }
      }
      true
    } else false
  }

  /**
   * Delete the log files prior the specified `oldestItemId`.
   */
  private[log] def cleanupLogFiles(): Iterable[File] = {
    import com.wajam.commons.Closable.using

    // Compute the 'safe' timestamp from which is ok to delete log files.
    // 1. find the log file which potentially contains the oldest item
    // 2. find the consistent timestamp of the initial record of that file
    def safeTimestamp: Option[Timestamp] = {
      oldestTimestamp.flatMap(txLog.guessLogFile).map(file => txLog.getIndexFromName(file.getName)) match {
        case Some(index) => {
          using(txLog.read(index)) { itr => if (itr.hasNext) itr.next().consistentTimestamp else None }
        }
        case None => None
      }
    }

    // Only delete files prior the computed 'safe' timestamp
    val filesToDelete = for {
      timestamp <- safeTimestamp.toList
      maxCleanupFile <- txLog.guessLogFile(timestamp).toList
      file <- txLog.getLogFiles.takeWhile(_.getCanonicalPath < maxCleanupFile.getCanonicalPath)
    } yield file
    filesToDelete.foreach(_.delete())
    filesToDelete
  }

}
