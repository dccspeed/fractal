package br.ufmg.cs.systems.fractal.util.collection

import java.io.{DataInput, DataOutput}

import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import org.apache.hadoop.io.Writable

import scala.collection.mutable.{PriorityQueue => SPriorityQueue}

class PriorityQueue [T <: Writable] (
      _maxSize: Int, _queue: SPriorityQueue[T], _ord: Ordering[T])
    extends Writable with Serializable {

  protected var maxSize: Int = _maxSize

  protected var queue: SPriorityQueue[T] = _queue

  protected var ord: Ordering[T] = _ord

  def this() = {
    this(-1, null, null)
  }

  def clear(): Unit = {
    queue.clear()
  }

  def enqueue(v: T): Boolean = {
    queue.enqueue(v)
    if (queue.size > maxSize) {
      queue.dequeue()
      false
    } else {
      true
    }
  }

  def merge(other: PriorityQueue[T]): Unit = {
    val iter = other.queue.iterator
    while (iter.hasNext) {
      enqueue(iter.next)
    }
  }

  def foreach(f: T => Unit): Unit = {
    queue.foreach(f)
  }
      
  override def write(out: DataOutput): Unit = {
    out.writeInt(maxSize)
    out.writeInt(queue.size)
    val ordBytes = SparkConfiguration.serialize(ord)
    out.writeInt(ordBytes.length)
    out.write(ordBytes)
    if (queue.size > 0) {
      var classWritten = false
      queue.foreach { v =>
        if (!classWritten) {
          val clazzBytes = SparkConfiguration.serialize(v.getClass)
          out.writeInt(clazzBytes.length)
          out.write(clazzBytes)
          classWritten = true
        }
        v.write(out)
      }
    }
  }

  override def readFields(in: DataInput): Unit = {
    maxSize = in.readInt()
    val size = in.readInt()
    val ordBytes = new Array[Byte](in.readInt())
    in.readFully(ordBytes)
    ord = SparkConfiguration.deserialize [Ordering[T]] (ordBytes)
    queue = new SPriorityQueue()(ord)
    if (size > 0) {
      val clazzBytes = new Array[Byte](in.readInt())
      in.readFully(clazzBytes)
      val clazz = SparkConfiguration.deserialize [Class[T]] (clazzBytes)
      var i = 0
      while (i < size) {
        val value = clazz.newInstance()
        value.readFields(in)
        enqueue(value)
        i += 1
      }
    }
  }
}

object PriorityQueue {
  def apply[T <: Writable](maxSize: Int, v: T, ord: Ordering[T]): PriorityQueue[T] = {
    val queue = new PriorityQueue [T] (maxSize, new SPriorityQueue()(ord), ord)
    queue.enqueue(v)
    queue
  }
}
