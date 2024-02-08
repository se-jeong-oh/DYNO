import java.io._
import java.nio.file._
import java.lang.Thread
import java.net._

import scala.collection.mutable.Map
import scala.util.control.Breaks._


object ScalaInference {
    def main(args: Array[String]): Unit = {
        // Write Request File to DNN model
        // val requestPath = Paths.get("/home/sej/data/pipe/request")
        // Read Result from DNN model
        // val resultPath = Paths.get("/home/sej/data/pipe/result")
        // message to Request
        // Request Information : requestID, inputRow, opName, p_opName, mapDevice, p_mapDeivce
        // val message = "1000,HashAggregate,ShuffleCoalesce,CPU,GPU"
        val length = 10
        val host = "localhost"
        val port = 12345

        // (CC, CG, GC, GG) // currOp, prevOp
        val estimatedExecMap = Map.empty[Int, Map[String, Tuple4[Float, Float, Float, Float]]]
/*
        // val resultFile = new File("/home/sej/data/pipe/result")
        // if (resultFile.exists()) {
        //     resultFile.delete()
        // }
        // Files.createFile(requestPath)
        // Files.createFile(resultPath)

        val writerRun = new Runnable {
            override def run(): Unit = {
                val writer = new PrintWriter(new File(requestPath.toString))
                while (true) {
                    for (i <- 1 to length) {
                        writer.println(i+","+message)
                        writer.flush()
                    }
                    writer.println("end")
                    writer.flush()
                    Thread.sleep(5000)
                }
                writer.close()
            }
        }
*/
        val readerRun = new Runnable {
            override def run(): Unit = {
                while(true) {
                    val socket = new Socket(host, port)
                    val inputStream = socket.getInputStream
                    val reader = new BufferedReader(new InputStreamReader(inputStream))
                    val line = reader.readLine()
                    println(s"Received: $line")
                    changeDictToMap(line)
                    socket.close()
                    Thread.sleep(100)
                }
            }
        }

        // val writerThread = new Thread(writerRun)
        // Thread.sleep(2000)
        val line = "{'CC13': 9.318072, 'CG13': 12.092408, 'GC13': 11.513946, 'GG13': 11.513946, 'CC12': 24.232252, 'CG12': 24.232252, 'GC12': 24.232252, 'GG12': 24.232252, 'CC11': 11.679304, 'CG11': 11.679304, 'GC11': 11.679304, 'GG11': 11.679304, 'CC10': 49.922493, 'CG10': 49.922493, 'GC10': 49.922493, 'GG10': 49.922493, 'CC9': 76.8176, 'CG9': 76.8176, 'GC9': 76.8176, 'GG9': 76.8176, 'CC8': 76.8176, 'CG8': 76.8176, 'GC8': 76.8176, 'GG8': 76.8176, 'CC7': 84.98161, 'CG7': 84.98161, 'GC7': 84.98161, 'GG7': 84.98161, 'CC6': 113.360016, 'CG6': 113.360016, 'GC6': 113.360016, 'GG6': 113.360016, 'CC5': 120.54269, 'CG5': 120.54269, 'GC5': 120.54269, 'GG5': 120.54269, 'CC4': 151.46573, 'CG4': 151.46573, 'GC4': 151.46573, 'GG4': 151.46573, 'CC3': 171.00533, 'CG3': 171.00533, 'GC3': 171.00533, 'GG3': 171.00533, 'CC2': 218.23895, 'CG2': 218.23895, 'GC2': 218.23895, 'GG2': 218.23895, 'CC1': 230.14568, 'CG1': 230.14568, 'GC1': 230.14568, 'GG1': 230.14568, 'CC0': 230.14568, 'CG0': 230.14568, 'GC0': 230.14568, 'GG0': 230.14568}"
        changeDictToMap(line)

        //val readerThread = new Thread(readerRun)

        //writerThread.start()
        //readerThread.start()
        //readerThread.join()
        //writerThread.join()
        
    }

    def changeDictToMap(line: String): Unit = {
        val keyValuePairs = line.stripMargin
                                .stripPrefix("{")
                                .stripSuffix("}")
                                .split(",")
                                .map { pair =>
                                    val Array(key, value) = pair.trim.stripPrefix("'").split(":")
                                    (key.trim.stripSuffix("'"), value.trim.toFloat)
                                }
        val scalaMap = Map(keyValuePairs: _*)
        println(scalaMap)
    }
}