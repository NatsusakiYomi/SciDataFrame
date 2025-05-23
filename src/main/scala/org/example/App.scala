import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.message.ArrowBlock
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import java.io.{File, FileInputStream, IOException}
import java.nio.{ByteBuffer, DirectByteBufferR, MappedByteBuffer}
import java.nio.channels.FileChannel.MapMode
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters._

object App {
  def main(args: Array[String]): Unit = {
    val runtime = Runtime.getRuntime
    printMemoryUsage(runtime, "程序开始")
    val file = new File("C:\\Users\\NatsusakiYomi\\Documents\\Study\\Postgrad1\\research\\ARROW\\bigfile.arrow")
    var rootAllocator: BufferAllocator = null
    var fileInputStream: FileInputStream = null
    var reader: ArrowFileReader = null
    var buf: MappedByteBuffer = null
    try {
      rootAllocator = new RootAllocator()
      fileInputStream = new FileInputStream(file)
      val buf = fileInputStream.getChannel.map(MapMode.READ_ONLY, 0, file.length());
      printMemoryUsage(runtime, "分配buf后");
      val seekableByteChannel = new MappedSeekableByteChannel(buf)

      reader = new ArrowFileReader(new SeekableReadChannel(seekableByteChannel), rootAllocator)

      println(s"Record batches in file: ${reader.getRecordBlocks.size()}")
      reader.getRecordBlocks.asScala.foreach { arrowBlock =>
        reader.loadRecordBatch(arrowBlock)
//        reader.loadNextBatch()
        printMemoryUsage(runtime, "分配recode batch后")
        val vectorSchemaRootRecover: VectorSchemaRoot = reader.getVectorSchemaRoot
//        println(vectorSchemaRootRecover.contentToTSVString())
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (reader != null) reader.close()
      if (fileInputStream != null) fileInputStream.close()
      if (rootAllocator != null) rootAllocator.close()
    }
  }
  private def printMemoryUsage(runtime: Runtime, phase: String): Unit = {
    val totalMemory = runtime.totalMemory()
    val freeMemory = runtime.freeMemory()
    val usedMemory = totalMemory - freeMemory

    println(s"$phase - 总内存: ${totalMemory / 1024 / 1024} MB, " +
      s"已用内存: ${usedMemory / 1024 / 1024} MB, " +
      s"空闲内存: ${freeMemory / 1024 / 1024} MB")
  }

  class MappedSeekableByteChannel(buffer: MappedByteBuffer) extends SeekableByteChannel {

    private val lock = new ReentrantLock()

    override def read(dst: ByteBuffer): Int = {
      lock.lock()
      try {
        if (buffer.remaining() == 0) return -1

        val bytesToRead = Math.min(buffer.remaining(), dst.remaining())
        val tempBuffer = new Array[Byte](bytesToRead)
        buffer.get(tempBuffer)
        dst.put(tempBuffer)
        bytesToRead
      } finally {
        lock.unlock()
      }
    }

    override def write(src: ByteBuffer): Int = {
      lock.lock()
      try {
        val bytesToWrite = Math.min(buffer.remaining(), src.remaining())
        val tempBuffer = new Array[Byte](bytesToWrite)
        src.get(tempBuffer)
        buffer.put(tempBuffer)
        bytesToWrite
      } finally {
        lock.unlock()
      }
    }


    override def position(): Long = {
      lock.lock()
      try {
        buffer.position()
      } finally {
        lock.unlock()
      }
    }

    override def position(newPosition: Long): SeekableByteChannel = {
      lock.lock()
      try {
        if (newPosition < 0 || newPosition > buffer.capacity()) throw new IllegalArgumentException("New position is out of bounds")
        buffer.position(newPosition.toInt)
        this
      } finally {
        lock.unlock()
      }
    }

    override def size(): Long = {
      lock.lock()
      try {
        buffer.capacity()
      } finally {
        lock.unlock()
      }
    }


    override def truncate(newSize: Long): SeekableByteChannel = {
      this
    }

    override def isOpen: Boolean = isOpen

    override def close(): Unit = {
      lock.lock()
      try {
      } finally {
        lock.unlock()
      }
    }

  }
}

