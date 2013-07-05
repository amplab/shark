package shark.memstore2.column

/** Enumerated values for compression/encoding schemes for columns.
  * The convention is to always capitalize when converting from String to CompressionScheme.Value
  */
object CompressionScheme extends Enumeration {
  val None   = Value("None")
  val Auto   = Value("Auto")
  val Dict   = Value("Dict")
  val RLE    = Value("Rle")
  val LZF    = Value("Lzf")
}
