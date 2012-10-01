package shark.memstore

trait ColumnFormat[T] {
  def apply(i: Int): Object
  def size: Int
  def append(v: T)
  def appendNull()
  def build: ColumnFormat[T]
}
