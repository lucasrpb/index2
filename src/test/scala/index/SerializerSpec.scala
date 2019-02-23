package index

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SerializerSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = Int.MaxValue

  implicit object DataBlockSerializer extends Serializer[Block[String, Int, Int]] {

    override def serialize(o: Block[String, Int, Int]): Array[Byte] = {

      val bos = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(bos)

      out.writeObject(o)
      out.flush()

      val r = bos.toByteArray()

      bos.close()

      r
    }

    override def deserialize(b: Array[Byte]): Option[Block[String, Int, Int]] = {
      val bis = new ByteArrayInputStream(b)
      val in = new ObjectInputStream(bis)

      val o = in.readObject().asInstanceOf[DataBlock[String, Int, Int]]

      in.close()

      Some(o)
    }
  }

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 10//rand.nextInt(4, 10)
    val META_ORDER = 10//rand.nextInt(4, 10)

    val DATA_MIN = DATA_ORDER - 1
    val DATA_MAX = DATA_ORDER*2 - 1

    val META_MIN = META_ORDER - 1
    val META_MAX = META_ORDER*2 - 1

    val b1 = new DataBlock[String, Int, Int](UUID.randomUUID.toString, DATA_MIN, DATA_MAX)
    val b2 = new DataBlock[String, Int, Int](UUID.randomUUID.toString, DATA_MIN, DATA_MAX)

    def insert(b: DataBlock[String, Int, Int]): DataBlock[String, Int, Int] = {
      val n = DATA_MAX
      var list = Seq.empty[(Int, Int)]

      for(i<-0 until n){
        val k = rand.nextInt(0, MAX_VALUE)

        if(!list.exists(_._1 == k)){
          list = list :+ k -> k
        }
      }

      b.insert(list)
      b
    }

    val s1 = DataBlockSerializer.serialize(insert(b1))
    val o1 = DataBlockSerializer.deserialize(s1)

    val s2 = DataBlockSerializer.serialize(insert(b2))
    val o2 = DataBlockSerializer.deserialize(s2)

    println(o1.get)
    println()
    println(o2.get)

  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()
    }

  }

}
