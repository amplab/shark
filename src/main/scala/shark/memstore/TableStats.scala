package shark.memstore


class TableStats(val stats: Array[Option[ColumnStats[_]]]) {

  override def toString =
    stats.zipWithIndex.map {
      case (column, index) =>
        "  column " + index + " " +
          (column match {
            case Some(column) => column.toString
            case _ => " no column statistics"
          })
    }.mkString("\n")

}
