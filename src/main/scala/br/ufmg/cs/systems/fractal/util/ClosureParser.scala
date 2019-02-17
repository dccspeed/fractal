package br.ufmg.cs.systems.fractal.util

import java.io._

import org.apache.xbean.asm5.Opcodes._
import org.apache.xbean.asm5.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.collection.mutable.{Set, Stack}
import scala.util.matching.{Regex => ScalaRegex}

/**
 * Closure parser based on spark's closure cleaner:
 * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/ClosureCleaner.scala
 */
object ClosureParser {

  implicit class Regex(sc: StringContext) {
    def r = new ScalaRegex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def getInnerClosureClassesAndMethods(obj: AnyRef)
    : (Set[Class[_]], Set[(String,String)]) = {
    val methods = Set[(String,String)]()
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val clazz = stack.pop()
      val cr = getClassReader(clazz)
      val set = Set[Class[_]]()
      // println (s"pop: ${clazz}")
      cr.accept(new InnerClassFinder(set, methods), 0)
      for (cls <- set -- seen) {
	seen += cls
	stack.push(cls)
        // println (s"\tfound: ${cls}")
      }
    }
    ( (seen - obj.getClass), methods )
  }

  private def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of
    // open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return new ClassReader(resourceStream)

    val baos = new ByteArrayOutputStream(128)
    copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

  private class InnerClassFinder(classes: Set[Class[_]],
      methods: Set[(String,String)])
    extends ClassVisitor(ASM5) {

    var myName: String = null

    override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
        myName = name
    }

    override def visitMethod(access: Int, name: String, desc: String,
        sig: String, exceptions: Array[String]): MethodVisitor = {

      methods += ( (name, desc) )
      println (s"${name} ${desc}")

      new MethodVisitor(ASM5) {
        override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
      

          val classTypes = Type.getArgumentTypes(desc) ++
            Array(Type.getReturnType(desc))
          classTypes.map (_.toString).foreach { t =>
            t match {
              case r"L(.*)$classPath;" =>
                classes += Class.forName (
                  classPath.replace("/", "."),
                  true,
                  Thread.currentThread.getContextClassLoader)
                case _ =>
            }
          }
        }
      }
    }
  }

  // TODO: make this method cleaner 
  def copyStream(in: InputStream, out: OutputStream,
      closeStreams: Boolean = false, transferToEnabled: Boolean = false
    ): Long = {
    var count = 0L
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] &&
        out.isInstanceOf[FileOutputStream] && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy
        // performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val initialPos = outChannel.position()
        val size = inChannel.size()

        // In case transferTo method transferred less data than we have
        // required.
        while (count < size) {
          count += inChannel.transferTo(count, size - count, outChannel)
        }

        // Check the position after transferTo loop to see if it is in the right
        // position and give user information if not.  Position will not be
        // increased to the expected length after calling transferTo in kernel
        // version 2.6.32, this issue can be seen in
        // https://bugs.openjdk.java.net/browse/JDK-7052359 This will lead to
        // stream corruption issue when using sort-based shuffle (SPARK-3948).
        val finalPos = outChannel.position()
        assert(finalPos == initialPos + size,
          s"""
          | Current position $finalPos do not equal to expected position
          | ${initialPos + size} after transferTo, please check your kernel
          | version to see if it is 2.6.32,
          | this is a kernel bug which will lead to unexpected behavior when
          | using transferTo. You can set spark.file.transferTo = false to
          | disable this NIO feature.
          """.stripMargin)
      } else {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
      }
      count
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  // TODO: make this method cleaner 
  private def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            println(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }

}
