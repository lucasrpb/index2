package index

object QueryAPI {

  def inOrder[T, K, V](start: Option[Block[T, K, V]]): Seq[(K, V)] = {
    start match {
      case None => Seq.empty[(K, V)]
      case Some(start) => start match {
        case leaf: Partition[T, K, V] => leaf.inOrder()
        case meta: MetaBlock[T, K, V] =>
          meta.pointers.slice(0, meta.size).foldLeft(Seq.empty[(K, V)]) { case (b, (_, n)) =>
            b ++ inOrder(Some(n))
          }
      }
    }
  }

}
