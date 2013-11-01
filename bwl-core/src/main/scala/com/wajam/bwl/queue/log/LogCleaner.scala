package com.wajam.bwl.queue.log

import com.wajam.nrv.consistency.log.FileTransactionLog
import com.wajam.nrv.utils.timestamp.Timestamp
import java.io.File
import java.util.concurrent.atomic.AtomicLong
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
                 cleanFrequencyInMs: Long)(implicit random: Random = Random) extends CurrentTime with Logging {

  private val nextCleanupTime = new AtomicLong(currentTime + (math.abs(random.nextLong()) % cleanFrequencyInMs))

  private var running: Boolean = false

  /**
   * Trigger the cleanup job if the job scheduled time has come. Returns a true if the cleanup job has been
   * triggered by this call or None if not the case.
   */
  def tick(): Boolean = {
    val next = nextCleanupTime.get
    val now = currentTime
    // TODO: Need to discuss why the synchronized running flag is use in this condition check
    if (now >= next && !synchronized(running) && nextCleanupTime.compareAndSet(next, now + cleanFrequencyInMs)) {
      synchronized {
        // The running flag prevent concurrent cleanup if the `cleanFrequencyInMs` value is shorter than the cleanup
        // job elapsed time.
        if (!running) {
          running = true

          debug(s"Cleaning up '${txLog.service}' extra log files")

          import ExecutionContext.Implicits.global
          import scala.concurrent._
          val cleanupFuture = future(cleanupLogFiles())

          cleanupFuture onFailure {
            case error => {
              warn(s"Cleanup error '${txLog.service}': ", error)
              // TODO: Need to synchronize again? This is invoked asynchronously but only one future is executed at a time. @volatile perhaps?
              synchronized(running = false)
            }
          }
          cleanupFuture onSuccess {
            case deletedFiles => {
              debug(s"Cleanup '${txLog.service}' deleted ${deletedFiles.size} log file(s).")
              synchronized(running = false) // TODO: Idem
            }
          }
          true
        } else false
      }
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

    /*
    // TODO: why the following code doesn't compile? "type mismatch: found Iterable[File], required: Option[?]"
    val filesToDelete2 = for {
      timestamp <- safeTimestamp
      maxCleanupFile <- txLog.guessLogFile(timestamp)
      file <- txLog.getLogFiles.takeWhile(_.getCanonicalPath < maxCleanupFile.getCanonicalPath)
    } yield file
*/

    // Only delete files prior the computed 'safe' timestamp
    val filesToDelete: Option[List[File]] = for {
      timestamp <- safeTimestamp
      maxCleanupFile <- txLog.guessLogFile(timestamp)
    } yield txLog.getLogFiles.takeWhile(_.getCanonicalPath < maxCleanupFile.getCanonicalPath).toList

    filesToDelete.foreach(files => files.foreach(_.delete()))
    filesToDelete.getOrElse(Nil)
    /*
    // TODO: is the following match than the two lines above?
    filesToDelete match {
      case Some(files) => {
        files.foreach(_.delete())
        files
      }
      case None => Nil
    }
*/
  }

}
