package index

import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class MemoryContext[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                           val META_ORDER: Int,
                                                           val store: Storage[T, K, V])
                                                          (implicit val ec: ExecutionContext,
                                                           ord: Ordering[K]) extends TxContext [T, K, V]{

  assert(DATA_ORDER > 2 && META_ORDER > 2, "DATA_ORDER and META_ORDER must be greater than 2!")

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  override def createPartition(): Partition[T, K, V] = {
    val p = new DataBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)
    blocks.put(p.id, p)
    p
  }

  override def createMeta(): MetaBlock[T, K, V] = {
    val m = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], META_MIN, META_MAX)
    blocks.put(m.id, m)
    m
  }

  override def getBlock(id: T): Future[Option[Block[T, K, V]]] = {
    blocks.get(id) match {
      case None => store.get(id)
      case Some(b) => Future.successful(Some(b))
    }
  }

  override def getPartition(id: T): Future[Option[Partition[T, K, V]]] = {
    getBlock(id).map(_.map(_.asInstanceOf[Partition[T, K, V]]))
  }

  override def getMeta(id: T): Future[Option[MetaBlock[T, K, V]]] = {
    getBlock(id).map(_.map(_.asInstanceOf[MetaBlock[T, K, V]]))
  }

  override def copy(p: Partition[T, K, V]): Partition[T, K, V] = {
    //if(blocks.isDefinedAt(p.id)) return p

    val block = p.asInstanceOf[DataBlock[T, K, V]]
    val copy = createPartition().asInstanceOf[DataBlock[T, K, V]]

    copy.size = block.size

    for(i<-0 until block.size){
      copy.keys(i) = block.keys(i)
    }

    parents += copy.id -> parents(p.id)

    copy
  }

  override def copy(m: MetaBlock[T, K, V]): MetaBlock[T, K, V] = {
    //if(blocks.isDefinedAt(m.id)) return m

    val copy = createMeta()

    parents += copy.id -> parents(m.id)

    copy.size = m.size

    for(i<-0 until m.size){
      val (k, child) = m.pointers(i)
      copy.setChild(k, child, i)(this)
    }

    copy
  }

  override def split(l: Partition[T, K, V]): Partition[T, K, V] = {
    val left = l.asInstanceOf[DataBlock[T, K, V]]
    val right = createPartition().asInstanceOf[DataBlock[T, K, V]]

    val len = left.size
    val middle = len/2

    for(i<-middle until len){
      right.keys(i - middle) = left.keys(i)

      right.size += 1
      left.size -= 1
    }

    right
  }

  override def split(left: MetaBlock[T, K, V]): MetaBlock[T, K, V] = {
    val right = createMeta()

    val len = left.size
    val middle = len/2

    for(i<-middle until len){
      val (k, child) = left.pointers(i)
      right.setChild(k, child, i - middle)(this)

      right.size += 1
      left.size -= 1
    }

    right
  }
}
