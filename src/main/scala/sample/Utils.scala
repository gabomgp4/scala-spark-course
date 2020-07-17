package sample

import java.nio.charset.CodingErrorAction

import scala.io.Codec

object Utils {
  val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
}


