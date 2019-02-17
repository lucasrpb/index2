import scala.collection.concurrent.TrieMap

package object index {

  type Parents[T, K, V] = TrieMap[Block[T, K, V], (Option[MetaBlock[T, K, V]], Int)]

  case class TxContext[T, K, V](){
    val parents: Parents[T, K, V] = new Parents[T, K, V]()
    val blocks = new TrieMap[Block[T, K, V], Boolean]()
  }

  case class IndexRef[T, K, V](id: T, root: Option[Block[T, K, V]] = None, size: Int = 0)
}
