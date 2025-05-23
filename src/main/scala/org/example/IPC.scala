import java.io._
import java.net._
import java.util.Date

class IPC(port: Int) {
  var out: PrintWriter = _
  var in: BufferedReader = _
  var socket: Socket = _
  var serverSocket: ServerSocket = _

  try {
    serverSocket = new ServerSocket(port)
    socket = serverSocket.accept()
    out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream)), true)
    in = new BufferedReader(new InputStreamReader(socket.getInputStream))
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def send(msg: String): Unit = {
    out.println(msg)
  }

  def flush(): Unit = {
    out.flush()
  }

  @throws[Exception]
  def recv(): String = {
    in.readLine()
  }
}

object IPC {
  def main(args: Array[String]): Unit = {
    val port = 32000
    try {
      val fip = new IPC(port)
      val start = new Date().getTime
      println("Connected.")
      for (_ <- 0 until 50) {
        for (_ <- 0 until 100)
          fip.send("+")
        fip.send(".")
        fip.flush()
        val msg = fip.recv()
        println("msg:"+ msg)
      }
      val stop = new Date().getTime
      println((stop - start) / 1000.0)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    }
  }
}
