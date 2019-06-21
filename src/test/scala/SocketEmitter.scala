import java.io.{OutputStreamWriter, PrintWriter}
import java.net.ServerSocket
import java.util.Date
import java.text.SimpleDateFormat
import scala.io.Source

object SocketEmitter {


  /**
    * Input params
    *
    * port -- we create a socket on this port
    * fileName -- emit lines from this file ever sleep seconds
    * sleepSecsBetweenWrites -- amount to sleep before emitting
    */
  def main(args: Array[String]) {
    args match {
      case Array(port, fileName, sleepSecs) => {


        val serverSocket = new ServerSocket(port.toInt)
        val socket = serverSocket.accept()
        val out: PrintWriter =
            new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream))

        for (line <- Source.fromFile(fileName).getLines()) {
          println(s"input line: ${line.toString}")
          var items: Array[String] = line.split("\\|")
          items.foreach{item  =>
            val output = s"$item"
            println(
                s"emitting to socket at ${new Date().toString()}: $output")
            out.println(output)
            }
          out.flush()
          Thread.sleep(1000 * sleepSecs.toInt)
        }
      }
      case _ =>
        throw new RuntimeException(
          "USAGE socket <portNumber> <fileName> <sleepSecsBetweenWrites>")
    }

    Thread.sleep(9999999 *1000)  // sleep until we are killed
  }
}

