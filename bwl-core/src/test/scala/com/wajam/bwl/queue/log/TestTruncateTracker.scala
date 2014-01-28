package com.wajam.bwl.queue.log

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import java.io.{ PrintWriter, FileOutputStream, File }
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.scalatest.Matchers._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TestTruncateTracker extends FlatSpec {

  trait TempFile {
    def withFile(test: File => Any) {
      val tempDir: File = Files.createTempDirectory("TestTruncateTracker").toFile
      try {
        test(new File(tempDir, "test.truncate"))
      } finally {
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  "Tracker" should "not fail if persist file does not exist" in new TempFile {
    withFile { file =>
      file.delete()
      file should not be 'exists

      new TruncateTracker(file)
    }
  }

  it should "not fail if persist file exist and is empty" in new TempFile {
    withFile { file =>
      new FileOutputStream(file).close()
      file should be('exists)

      new TruncateTracker(file)
    }
  }

  it should "not fail if persisted file is corrupted" in new TempFile {
    withFile { file =>

      import com.wajam.commons.Closable.using

      val random = new Random(999)
      val buffer = new Array[Byte](256)
      using(new FileOutputStream(file)) { out =>
        random.nextBytes(buffer)
        out.write(buffer)
        random.nextBytes(buffer)
        out.write(buffer)
        random.nextBytes(buffer)
        out.write(buffer)
      }

      new TruncateTracker(file)
    }
  }

  it should "ignore corrupted timestamps" in new TempFile {
    withFile { file =>

      import com.wajam.commons.Closable.using

      using(new PrintWriter(file, "UTF-8")) { writer =>
        writer.println("1")
        writer.println("2")
        writer.println("a")
        writer.println("3")
      }
      val tracker = new TruncateTracker(file)
      tracker.contains(1L) should be(true)
      tracker.contains(2L) should be(true)
      tracker.contains(3L) should be(true)
    }
  }

  it should "write truncated timestamps and read them back when created" in new TempFile {
    withFile { file =>
      val tracker1 = new TruncateTracker(file)
      tracker1.contains(1L) should be(false)
      tracker1.truncate(1L)
      tracker1.contains(1L) should be(true)
      tracker1.contains(2L) should be(false)
      tracker1.truncate(2L)
      tracker1.contains(2L) should be(true)
      tracker1.contains(3L) should be(false)

      val tracker2 = new TruncateTracker(file)
      tracker2.contains(1L) should be(true)
      tracker2.contains(2L) should be(true)
      tracker2.contains(3L) should be(false)
    }
  }

  it should "clean truncated timestamps older than oldest timestamp when compacting" in new TempFile {
    withFile { file =>
      val tracker1 = new TruncateTracker(file)
      tracker1.truncate(10L)
      tracker1.truncate(60L)
      tracker1.truncate(30L)
      tracker1.truncate(20L)
      tracker1.truncate(50L)
      tracker1.truncate(40L)
      tracker1.compact(Some(35L))
      tracker1.contains(10L) should be(false)
      tracker1.contains(20L) should be(false)
      tracker1.contains(30L) should be(false)
      tracker1.contains(40L) should be(true)
      tracker1.contains(50L) should be(true)
      tracker1.contains(60L) should be(true)

      val tracker2 = new TruncateTracker(file)
      tracker2.contains(10L) should be(false)
      tracker2.contains(20L) should be(false)
      tracker2.contains(30L) should be(false)
      tracker2.contains(40L) should be(true)
      tracker2.contains(50L) should be(true)
      tracker2.contains(60L) should be(true)
      tracker2.compact(Some(50L))
      tracker2.contains(40L) should be(false)
      tracker2.contains(50L) should be(true)
      tracker2.contains(60L) should be(true)
    }
  }
}
