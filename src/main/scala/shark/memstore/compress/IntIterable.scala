package shark.memstore.compress


trait IntIterable {
  def size: Int
  def iterator: IntIterator
}

trait IntIterator {
  def next: Int
}
