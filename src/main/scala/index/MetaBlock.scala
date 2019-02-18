package index

import java.util.UUID
import scala.reflect.ClassTag

class MetaBlock[T: ClassTag, K: ClassTag, V: ClassTag](val id: T,
                                                       val MIN: Int,
                                                       val MAX: Int)(implicit ord: Ordering[K]) extends Block[T, K, V]{

  val MIDDLE = MIN

  var size = 0
  val pointers = Array.ofDim[(K, T)](MAX)

  def find(k: K, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: K): Option[T] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, size - 1)

    Some(pointers(if(pos < size) pos else pos - 1)._2)
  }

  def setChild(k: K, child: T, pos: Int)(implicit ctx: TxContext[T, K, V]): Boolean = {
    pointers(pos) = k -> child

    ctx.parents += child -> (Some(id), pos)

    true
  }

  def left[S <: Block[T, K, V]](pos: Int): Option[S] = {
    val lpos = pos - 1
    if(lpos < 0) return None
    Some(pointers(lpos)._2.asInstanceOf[S])
  }

  def right[S <: Block[T, K, V]](pos: Int): Option[S] = {
    val rpos = pos + 1
    if(rpos >= size) return None
    Some(pointers(rpos)._2.asInstanceOf[S])
  }

  def insertAt(k: K, child: T, idx: Int)(implicit ctx: TxContext[T, K, V]): (Boolean, Int) = {
    for(i<-size until idx by -1){
      val (k, child) = pointers(i - 1)
      setChild(k, child, i)
    }

    setChild(k, child, idx)

    size += 1

    true -> idx
  }

  def insert(k: K, child: T)(implicit ctx: TxContext[T, K, V]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val (found, idx) = find(k, 0, size - 1)

    if(found) return false -> 0

    insertAt(k, child, idx)
  }

  def insert(data: Seq[(K, T)])(implicit ctx: TxContext[T, K, V]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val len = Math.min(MAX - size, data.length)

    for(i<-0 until len){
      val (k, child) = data(i)
      if(!insert(k, child)._1) return false -> 0
    }

    true -> len
  }

  def removeAt(idx: Int)(implicit ctx: TxContext[T, K, V]): (K, T) = {
    val data = pointers(idx)

    size -= 1

    for(i<-idx until size){
      val (k, child) = pointers(i + 1)
      setChild(k, child, i)
    }

    data
  }

  def remove(k: K)(implicit ctx: TxContext[T, K, V]): Boolean = {
    if(isEmpty()) return false

    val (found, idx) = find(k, 0, size - 1)

    if(!found) return false

    removeAt(idx)

    true
  }

  def remove(keys: Seq[K])(implicit ctx: TxContext[T, K, V]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    val len = keys.length

    for(i<-0 until len){
      if(!remove(keys(i))) return false -> 0
    }

    true -> len
  }

  def update(data: Seq[(K, T)])(implicit ctx: TxContext[T, K, V]): (Boolean, Int) = {

    val len = data.length

    for(i<-0 until len){
      val (k, child) = data(i)

      val (found, idx) = find(k, 0, size - 1)

      if(!found) return false -> 0

      setChild(k, child, idx)
    }

    true -> len
  }

  /*def split()(implicit ctx: TxContext[T, K, V]): MetaBlock[T, K, V] = {
    val right = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)

    val len = size
    val middle = len/2

    for(i<-middle until len){
      val (k, child) = pointers(i)
      right.setChild(k, child, i - middle)

      right.size += 1
      size -= 1
    }

    right
  }*/

  /*def copy()(implicit ctx: TxContext[T, K, V]): MetaBlock[T, K, V] = {

    if(ctx.blocks.isDefinedAt(this)) return this

    val copy = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)

    ctx.blocks += copy -> true
    ctx.parents += copy -> ctx.parents(this)

    copy.size = size

    for(i<-0 until size){
      val (k, child) = pointers(i)
      copy.setChild(k, child, i)
    }

    copy
  }*/

  override def max: Option[K] = {
    if(isEmpty()) return None
    Some(pointers(size - 1)._1)
  }

  override def isFull(): Boolean = size == MAX
  override def isEmpty(): Boolean = size == 0
  override def hasMinimumKeys(): Boolean = size >= MIN
  override def hasEnoughKeys(): Boolean = size > MIN

  def inOrder(): Seq[(K, T)] = {
    if(isEmpty()) return Seq.empty[(K, T)]
    pointers.slice(0, size)
  }

  override def toString(): String = {
    inOrder().map(_._1).mkString(s"(", "," ,")")
  }

}