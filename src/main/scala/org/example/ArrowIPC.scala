import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{FieldVector, IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowFileWriter, ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.io.FileInputStream
import java.net.{ServerSocket, Socket}
import java.nio.channels.Channels
import scala.collection.JavaConverters._

object ArrowIPC {
  def main(args: Array[String]): Unit = {
    val allocator: BufferAllocator = new RootAllocator()

    val field: Field = new Field("int_field", FieldType.nullable(new ArrowType.Int(32, true)), null)
    val schema: Schema = new Schema(List(field).asJava)

    val serverSocket = new ServerSocket(9090)
    println("Server is listening on port 9090")
//    val filePath = "C:\\Users\\Yomi\\PycharmProjects\\MemoryMap\\src\\main\\scala\\org\\example\\1.txt"

//    var fileInputStream = new FileInputStream(filePath)
//    var reader = new ArrowStreamReader(fileInputStream, allocator)
    var root = VectorSchemaRoot.create(schema,allocator)
    while (true) {
      val socket: Socket = serverSocket.accept()
      println("New client connected")
      root.allocateNew()
      val intVector = root.getVector(0).asInstanceOf[IntVector]
      intVector.setSafe(0,1)
      root.setRowCount(1)

      val writer = new ArrowFileWriter(root, null, Channels.newChannel(socket.getOutputStream))

      try {
        writer.start()
        writer.writeBatch()
//        while (reader.loadNextBatch) {
//          System.out.println("Read batch with " + root.getRowCount + " rows.")
//          root = reader.getVectorSchemaRoot
//          writer.writeBatch()
//        }
        writer.end()
      } finally {
        writer.close()
        root.close()
        socket.close()
      }
    }
  }
}

