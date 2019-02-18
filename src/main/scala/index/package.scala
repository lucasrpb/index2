import scala.collection.concurrent.TrieMap

package object index {

  type Parents[T, K, V] = TrieMap[T, (Option[T], Int)]
  case class IndexRef[T, K, V](id: T, root: Option[T] = None, size: Int = 0)

}
