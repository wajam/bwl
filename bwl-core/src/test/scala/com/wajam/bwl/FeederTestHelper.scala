package com.wajam.bwl

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable
import com.wajam.spnl.feeder.Feeder.FeederData
import language.implicitConversions

// TODO: Add in SPNL. This is a copy of a class also present in MRY
object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[FeederData]] with Closable = {

    new Iterator[Option[FeederData]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }
}