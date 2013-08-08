package shark.memstore2.column

import java.nio.ByteBuffer
import scala.annotation.tailrec
import java.nio.ByteOrder


trait CompressionAlgorithm  {

  def compressionType: CompressionType
  def supportsType(t: ColumnType[_]): Boolean
  def gatherStatsForCompressability[T](v: T, t: ColumnType[T])
  def compressibilityScore: Double
  def sizeOnCompression: Int
  def compress[T](b: ByteBuffer, t: ColumnType[T]): ByteBuffer
}

case class CompressionType(index: Int) {}
object RLECompressionType extends CompressionType(0)




class RLE extends CompressionAlgorithm {
  private var _count: Int = 0
  private var _total: Int = 0
  private var _prev: Any = _
  private var _size: Int = 0
  
  override def compressionType = RLECompressionType

  override def supportsType(t: ColumnType[_]) = {
    t match {
      case INT | STRING | SHORT | BYTE | BOOLEAN => true
      case _ => false
    }
  }
  override def gatherStatsForCompressability[T](v: T, t: ColumnType[T]) = {
    if (_prev == null) {
      _prev = v
      _size += t.actualSize(v)
    } else {
      if (_prev.equals(v)) {
        _count += 1
      } else {
        _size += t.actualSize(v)
      }
    }
    _total += 1
  }

  override def compressibilityScore = (_count / (_total + 0.0))
  
  override def sizeOnCompression = _size + _count*4

  override def compress[T](b: ByteBuffer, t: ColumnType[T]) = {
    val compressedBuffer = ByteBuffer.allocate(sizeOnCompression + 4 + 4)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(compressionType.index)
    compressedBuffer.putInt(b.getInt())
    encode(b, compressedBuffer, null, t)
    compressedBuffer.rewind()
    compressedBuffer
  }

  @tailrec private def encode[T](currentBuffer: ByteBuffer,
    compressedBuffer: ByteBuffer, currentRun: (T, Int), t: ColumnType[T]) {
    def writeOutRun() = {
      t.append(currentRun._1, compressedBuffer)
      compressedBuffer.putInt(currentRun._2)
    }
    if (!currentBuffer.hasRemaining()) {
      writeOutRun
      return
    }
    val elem = t.extract(currentBuffer.position(), currentBuffer)
    val newRun = if (currentRun == null) {
      (elem, 1)
    } else if (currentRun._1.equals(elem)) {
      //update length
      (currentRun._1, currentRun._2 + 1)
    } else {
      //write out the current run to compressed buffer
      writeOutRun()
      (elem, 1)
    }
    encode(currentBuffer, compressedBuffer, newRun, t)
  }
}