package io.arabesque.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.io.{ObjectInputStream, ObjectOutputStream}

abstract class Cloneable {
  
  lazy val baos = new ByteArrayOutputStream

  lazy val oos = new ObjectOutputStream(baos)
    
  lazy val bais = new ByteArrayInputStream(baos.toByteArray)
  
  lazy val ois = new ObjectInputStream(bais)

  private var serialized = false

  // auxiliary functions
  private def serialize(): Unit = {
    oos.writeObject(this)
    oos.close
    serialized = true
  }

  private def deserialize[T](): T = {
    ois.readObject().asInstanceOf[T]
  }

  def deepCopy[T](): T = {
    if (!serialized) {
      serialize()
    }
    deserialize[T]()
  }
}
