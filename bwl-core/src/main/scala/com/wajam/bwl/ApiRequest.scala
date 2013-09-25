package com.wajam.bwl

import com.wajam.nrv.data._
import com.wajam.nrv.InvalidParameter
import com.wajam.nrv.data.MList

// TODO: To merge with NRV Request
class ApiRequest(msg: InMessage) {
  def paramString(param: String): String = {
    paramString(param, throw new InvalidParameter(s"Parameter $param must be specified"))
  }

  def paramString(param: String, default: => String): String = paramOptionalString(param).getOrElse(default)

  def paramOptionalString(param: String): Option[String] = paramOptionalSeqString(param) match {
    case Some(head :: _) => Some(head)
    case _ => None
  }

  def paramOptionalSeqString(param: String): Option[Seq[String]] = msg.parameters.get(param).map {
    case MList(values) => values.map(_.toString).toSeq
    case value: MValue => Seq(value.toString)
    case value => throw new InvalidParameter(s"Parameter $param unsupported value $value")
  }

  def paramBoolean(param: String): Boolean = {
    paramBoolean(param, throw new InvalidParameter(s"Parameter $param must be specified"))
  }

  def paramBoolean(param: String, default: => Boolean): Boolean = paramOptionalBoolean(param).getOrElse(default)

  def paramOptionalBoolean(param: String): Option[Boolean] = paramOptionalString(param).map {
    case "1" => true
    case "0" => false
    case "true" => true
    case "false" => false
    case "t" => true
    case "f" => false
    case value => throw new InvalidParameter(s"Parameter $param unsupported value $value")
  }

  def paramLong(param: String): Long = {
    paramLong(param, throw new InvalidParameter(s"Parameter $param must be specified"))
  }

  def paramLong(param: String, default: => Long): Long = paramOptionalLong(param).getOrElse(default)

  def paramOptionalLong(param: String): Option[Long] = {
    try {
      paramOptionalString(param).map(_.toLong)
    } catch {
      case e: Exception => throw new InvalidParameter(s"Parameter $param must be numeric")
    }
  }

  def paramInt(param: String): Int = {
    paramInt(param, throw new InvalidParameter(s"Parameter $param must be specified"))
  }

  def paramInt(param: String, default: => Int): Int = paramOptionalInt(param).getOrElse(default)

  def paramOptionalInt(param: String): Option[Int] = {
    val value = paramOptionalLong(param)
    try {
      value.map(_.toInt)
    } catch {
      case e: Exception => throw new InvalidParameter(s"Parameter $param unsupported value $value")
    }
  }

  def respond(response: Any, headers: Map[String, MValue] = Map(), code: Int = 200) {
    msg.reply(headers, meta = null, data = response, code = code)
  }

  def respondException(e: Exception) {
    val realError = e.getCause match {
      case null => e
      case cause: Exception => cause
      case _ => e
    }

    msg.replyWithError(realError)
  }
}
