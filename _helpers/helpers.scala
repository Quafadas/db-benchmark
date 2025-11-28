import scala.sys.process._
import java.io._
import java.time._
import java.nio.file._

object BenchmarkHelpers {

  def writeLog(
    task: String,
    data: String,
    inRows: Long,
    question: String,
    outRows: Long,
    outCols: Int,
    solution: String,
    version: String,
    git: String,
    fun: String,
    run: Int,
    timeSec: Double,
    memGb: Double,
    cache: String,
    chk: String,
    chkTimeSec: Double,
    onDisk: String,
    machineType: String
  ): Unit = {
    val batch = sys.env.getOrElse("BATCH", "")
    val timestamp = System.currentTimeMillis() / 1000.0
    val csvFile = sys.env.getOrElse("CSV_TIME_FILE", "time.csv")
    val nodename = java.net.InetAddress.getLocalHost().getHostName()
    val comment = "" // placeholder for updates to timing data

    val logRow = Array(
      nodename, batch, timestamp.toString, task, data, inRows.toString,
      question, outRows.toString, outCols.toString, solution, version, git,
      fun, run.toString, f"$timeSec%.3f", f"$memGb%.3f", cache, chk,
      f"$chkTimeSec%.3f", comment, onDisk, machineType
    ).mkString(",")

    val logHeader = Array(
      "nodename", "batch", "timestamp", "task", "data", "in_rows",
      "question", "out_rows", "out_cols", "solution", "version", "git",
      "fun", "run", "time_sec", "mem_gb", "cache", "chk", "chk_time_sec",
      "comment", "on_disk", "machine_type"
    ).mkString(",")

    val file = new File(csvFile)
    val append = file.exists() && file.length() > 0

    val csvVerbose = sys.env.getOrElse("CSV_VERBOSE", "false").toLowerCase == "true"
    if (csvVerbose) {
      println(s"# $logRow")
    }

    val writer = new BufferedWriter(new FileWriter(file, append))
    try {
      if (!append) {
        writer.write(logHeader)
        writer.newLine()
      }
      writer.write(logRow)
      writer.newLine()
    } finally {
      writer.close()
    }
  }

  def makeChk(values: Seq[Any]): String = {
    values.map {
      case d: Double => f"$d%.3f"
      case f: Float => f"$f%.3f"
      case x => x.toString
    }.mkString(";").replace(",", "_")
  }

  def memoryUsage(): Double = {
    val runtime = Runtime.getRuntime
    val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0 * 1024.0)
    usedMemory
  }

  def timeIt[T](block: => T): (T, Double) = {
    val start = System.nanoTime()
    val result = block
    val elapsed = (System.nanoTime() - start) / 1e9
    (result, elapsed)
  }
}
