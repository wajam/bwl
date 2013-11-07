package com.wajam.bwl.queue.log

import java.io._
import com.wajam.nrv.utils.timestamp.Timestamp
import scala.collection.immutable.TreeSet
import scala.io.Source
import com.wajam.commons.Logging

/**
 * This class track deleted queue items. Since individual items cannot be removed from the log, a list of truncated
 * items is kept separately. The items contained in this list are filtered out once read from the log. The truncate
 * list is persisted on disk and survives server restart.
 */
class TruncateTracker(persistFile: File) extends Logging {

  @volatile
  private var truncated: TreeSet[Timestamp] = read()

  /**
   * Returns true if the specified timestamp is in the truncate list
   */
  def contains(timestamp: Timestamp): Boolean = truncated.contains(timestamp)

  /**
   * Add specified timestamp in truncate list
   */
  def truncate(timestamp: Timestamp) = synchronized {
    import com.wajam.commons.Closable.using

    using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(persistFile, true), "UTF-8"))) { out =>
      out.println(timestamp.toString())
      out.flush()
    }

    truncated += timestamp
  }

  /**
   * Remove timestamps older than the specified `oldestConsistentTimestamp` and write the resulting list on disk.
   */
  def compact(oldestConsistentTimestamp: Option[Timestamp]): Int = {

    oldestConsistentTimestamp match {
      case Some(oldest) => {
        debug(s"Compacting truncate file '${persistFile.getName}' older than $oldestConsistentTimestamp")

        val count = synchronized {
          val original = truncated
          truncated = original.dropWhile(_ < oldest)
          write(truncated)
          original.size - truncated.size
        }

        debug(s"Truncate file '${persistFile.getName}' compacted. $count timestamp(s) older than $oldestConsistentTimestamp removed")
        count
      }
      case None => 0
    }
  }

  private def read(): TreeSet[Timestamp] = {
    import com.wajam.commons.Closable.using

    try {
      using(new FileInputStream(persistFile)) { in =>
        val lines = Source.fromInputStream(in, "UTF-8").getLines()
        val timestamps = lines.collect { case value if value.forall(_.isDigit) => Timestamp(value.toLong) }
        new TreeSet[Timestamp]() ++ timestamps
      }
    } catch {
      case e: FileNotFoundException => new TreeSet[Timestamp]()
      case e: Exception => {
        warn(s"Error reading truncate file '${persistFile.getName}'", e)
        new TreeSet[Timestamp]()
      }
    }
  }

  private def write(timestamps: Iterable[Timestamp]) = {
    import com.wajam.commons.Closable.using

    using(new PrintWriter(persistFile, "UTF-8")) { out =>
      timestamps.foreach(timestamp => out.println(timestamp.toString()))
      out.flush()
    }
  }
}
