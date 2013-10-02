package com.wajam.bwl

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable

// TODO: Add in SPNL. This is a copy of a class also present in MRY
object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[Map[String, Any]]] with Closable = {

    new Iterator[Option[Map[String, Any]]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }
}