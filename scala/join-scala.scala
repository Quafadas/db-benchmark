#!/usr/bin/env -S scala-cli shebang

@main def join =
  println("# join-scala.scala")

  val ver = "0.1.0"
  val git = ""
  val task = "join"
  val solution = "scala"
  val fun = ".join"
  val cache = "TRUE"

  val dataName = sys.env.getOrElse("SRC_DATANAME", "J1_1e7_NA_0_0")
  val machineType = sys.env.getOrElse("MACHINE_TYPE", "local")

  println(s"Hello from Scala!")
  println(s"Data: $dataName")
  println(s"Machine: $machineType")
  println(s"Task: $task")
  println(s"Solution: $solution v$ver")

  // TODO: Implement actual join benchmarks
  // This is a placeholder to verify the setup works
