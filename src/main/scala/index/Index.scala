package index

import java.util.UUID
import scala.reflect.ClassTag

class Index[T: ClassTag, K: ClassTag, V: ClassTag](var iref: IndexRef[T, K, V],
                                                   val DATA_ORDER: Int,
                                                   val META_ORDER: Int)(implicit ord: Ordering[K]){

  assert(DATA_ORDER > 2 && META_ORDER > 2, "DATA_ORDER and META_ORDER must be greater than 2!")

  val id = UUID.randomUUID.toString.asInstanceOf[T]

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  var root = iref.root
  var size = iref.size

  implicit val ctx = new TxContext[T, K, V]()

  def ref: IndexRef[T, K, V] = IndexRef(id, root, size)

  def fixRoot(p: Block[T, K, V]): Boolean = {
    p match {
      case p: MetaBlock[T, K, V] =>

        if(p.size == 1){
          root = Some(p.pointers(0)._2)
          true
        } else {
          root = Some(p)
          true
        }

      case p: Partition[T, K, V] =>
        root = Some(p)
        true
    }
  }

  def recursiveCopy(p: Block[T, K, V]): Boolean = {
    val (parent, pos) = ctx.parents(p)

    parent match {
      case None => fixRoot(p)
      case Some(parent) =>
        val PARENT = parent.copy()
        PARENT.setChild(p.max.get, p, pos)
        recursiveCopy(PARENT)

    }
  }

  def find(k: K, start: Option[Block[T, K, V]]): Option[Partition[T, K, V]] = {
    start match {
      case None => None
      case Some(start) => start match {
        case leaf: DataBlock[T, K, V] => Some(leaf)
        case meta: MetaBlock[T, K, V] =>

          val size = meta.size
          val pointers = meta.pointers

          for(i<-0 until size){
            val child = pointers(i)._2
            ctx.parents += child -> (Some(meta), i)
          }

          find(k, meta.findPath(k))
      }
    }
  }

  def find(k: K): Option[Partition[T, K, V]] = {
    if(root.isDefined){
      ctx.parents += root.get -> (None, 0)
    }

    find(k, root)
  }

  def insertEmptyIndex(data: Seq[(K, V)]): (Boolean, Int) = {
    val p = new DataBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)

    val (ok, n) = p.insert(data)
    ctx.parents += p -> (None, 0)

    (ok && recursiveCopy(p)) -> n
  }

  def insertParent(left: MetaBlock[T, K, V], prev: Block[T, K, V]): Boolean = {
    if(left.isFull()){
      val right = left.split()

      if(ord.gt(prev.max.get, left.max.get)){
        right.insert(prev.max.get, prev)
      } else {
        left.insert(prev.max.get, prev)
      }

      return handleParent(left, right)
    }

    left.insert(prev.max.get, prev)

    recursiveCopy(left)
  }

  def handleParent(left: Block[T, K, V], right: Block[T, K, V]): Boolean = {
    val (parent, pos) = ctx.parents(left)

    parent match {
      case None =>

        val meta = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], META_MIN, META_MAX)

        ctx.parents += meta -> (None, 0)

        meta.insert(Seq(
          left.max.get -> left,
          right.max.get -> right
        ))

        recursiveCopy(meta)

      case Some(parent) =>

        val PARENT = parent.copy()
        PARENT.setChild(left.max.get, left, pos)

        insertParent(PARENT, right)
    }
  }

  def insertLeaf(leaf: Partition[T, K, V], data: Seq[(K, V)]): (Boolean, Int) = {
    val left = copyPartition(leaf)

    if(leaf.isFull()){
      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.insert(data)

    (ok && recursiveCopy(left)) -> n
  }

  def insert(data: Seq[(K, V)]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmptyIndex(list)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    this.size += size

    println(s"inserting ${data.map(_._1)}...\n")

    true -> size
  }

  def copyPartition(p: Partition[T, K, V]): Partition[T, K, V] = {
    if(ctx.blocks.isDefinedAt(p)) return p

    val copy = p.copy()

    ctx.blocks += copy -> true
    ctx.parents += copy -> ctx.parents(p)

    copy
  }

  def update(data: Seq[(K, V)]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => false -> 0
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
          if(idx > 0) list = list.slice(0, idx)

          val left = copyPartition(leaf)

          left.update(list) match {
            case (true, count) => recursiveCopy(left) -> count
            case _ => false -> 0
          }
      }

      if(!ok) return false -> 0

      pos += n
    }

    // println(s"updating ${data}...\n")

    true -> size
  }

}
