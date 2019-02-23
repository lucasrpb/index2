package index

import java.nio.ByteBuffer

trait Serializer[T] {

  def serialize(o: T): Array[Byte]
  def deserialize(b: Array[Byte]): Option[T]

}
