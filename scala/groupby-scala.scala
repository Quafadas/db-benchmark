#!/usr/bin/env -S scala-cli shebang

import io.github.quafadas.table.{*, given}
import vecxt.all.{given, *}

@main def groupby: Unit =
  println("# groupby-scala.scala")
  System.out.flush()

  val ver = "0.1.0"
  val git = ""
  val task = "groupby"
  val solution = "scala"
  val fun = ".groupby"
  val cache = "TRUE"

  val dataName = sys.env.getOrElse("SRC_DATANAME", "G1_1e7_1e2_0_0")
  val machineType = sys.env.getOrElse("MACHINE_TYPE", "local")
  inline val srcGrp = "/Users/simon/Code/db-benchmark/data/G1_1e7_1e2_0_0.csv"

  println(s"loading dataset $dataName")
  System.out.flush()

  // Load CSV data using scautable
  val df = CSV.absolutePath(srcGrp, TypeInferrer.FirstN(1000))

  // Convert to case class sequence - scautable provides typed access
  val data = df.toArray // Load into memory

  val inRows = data.length.toLong
  val onDisk = "FALSE"

  println(inRows)
  System.out.flush()

  val taskInit = System.nanoTime()
  println("grouping...")
  System.out.flush()

  // Question 1: sum v1 by id1
  var question = "sum v1 by id1"
  System.gc()
  var (ans1, t) = BenchmarkHelpers.timeIt {
    data.groupMapReduce(_.id1)(_.v1.toLong)(_ + _)
  }
  println(s"(${ans1.size}, 2)")
  System.out.flush()
  var m = BenchmarkHelpers.memoryUsage()
  val chkResult = BenchmarkHelpers.timeIt {
    Seq(ans1.values.sum)
  }
  val chk = chkResult._1
  val chkt = chkResult._2
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans1.size.toLong, 2,
    solution, ver, git, fun, 1, t, m, cache, BenchmarkHelpers.makeChk(chk), chkt, onDisk, machineType)

  System.gc()
  val (ans1b, t2) = BenchmarkHelpers.timeIt {
    data.groupMapReduce(_.id1)(_.v1.toLong)(_ + _)
  }
  println(s"(${ans1b.size}, 2)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk2, chkt2) = BenchmarkHelpers.timeIt {
    Seq(ans1b.values.sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans1b.size.toLong, 2,
    solution, ver, git, fun, 2, t2, m, cache, BenchmarkHelpers.makeChk(chk2), chkt2, onDisk, machineType)
  println(ans1b.take(3))
  System.out.flush()

  // Question 2: sum v1 by id1:id2
  question = "sum v1 by id1:id2"
  System.gc()
  val (ans2, t3) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id1, r.id2)).view.mapValues(_.map(_.v1.toLong).sumSIMD).toMap
  }
  println(s"(${ans2.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk3, chkt3) = BenchmarkHelpers.timeIt {
    Seq(ans2.values.sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans2.size.toLong, 3,
    solution, ver, git, fun, 1, t3, m, cache, BenchmarkHelpers.makeChk(chk3), chkt3, onDisk, machineType)

  System.gc()
  val (ans2b, t4) = BenchmarkHelpers.timeIt {
    data.groupMapReduce(r => (r.id1, r.id2))(_.v1.toLong)(_ + _)
  }
  println(s"(${ans2b.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk4, chkt4) = BenchmarkHelpers.timeIt {
    Seq(ans2b.values.sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans2b.size.toLong, 3,
    solution, ver, git, fun, 2, t4, m, cache, BenchmarkHelpers.makeChk(chk4), chkt4, onDisk, machineType)
  println(ans2b.take(3))
  System.out.flush()

  // Question 3: sum v1 mean v3 by id3
  question = "sum v1 mean v3 by id3"
  System.gc()
  val (ans3, t5) = BenchmarkHelpers.timeIt {
    data.groupMapReduce(_.id3)(r => (r.v1.toLong, r.v3, 1)) { case ((sum1a, sum3a, ca), (sum1b, sum3b, cb)) =>
      (sum1a + sum1b, sum3a + sum3b, ca + cb)
    }.view.mapValues { case (sumV1, sumV3, count) =>
      (sumV1, sumV3 / count)
    }.toMap
  }
  println(s"(${ans3.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk5, chkt5) = BenchmarkHelpers.timeIt {
    Seq(ans3.values.map(_._1).sum, ans3.values.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans3.size.toLong, 3,
    solution, ver, git, fun, 1, t5, m, cache, BenchmarkHelpers.makeChk(chk5), chkt5, onDisk, machineType)

  System.gc()
  val (ans3b, t6) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id3).view.mapValues { grp =>
      (grp.map(_.v1.toLong).sumSIMD, grp.map(_.v3).sumSIMD / grp.size)
    }.toMap
  }
  println(s"(${ans3b.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk6, chkt6) = BenchmarkHelpers.timeIt {
    Seq(ans3b.values.map(_._1).sum, ans3b.values.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans3b.size.toLong, 3,
    solution, ver, git, fun, 2, t6, m, cache, BenchmarkHelpers.makeChk(chk6), chkt6, onDisk, machineType)
  println(ans3b.take(3))
  System.out.flush()

  // Question 4: mean v1:v3 by id4
  question = "mean v1:v3 by id4"
  System.gc()
  val (ans4, t7) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id4).view.mapValues { grp =>
      (grp.map(_.v1).sumSIMD.toDouble / grp.size,
       grp.map(_.v2).sumSIMD.toDouble / grp.size,
       grp.map(_.v3).sumSIMD / grp.size)
    }.toMap
  }
  println(s"(${ans4.size}, 4)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk7, chkt7) = BenchmarkHelpers.timeIt {
    Seq(ans4.values.map(_._1).sum, ans4.values.map(_._2).sum, ans4.values.map(_._3).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans4.size.toLong, 4,
    solution, ver, git, fun, 1, t7, m, cache, BenchmarkHelpers.makeChk(chk7), chkt7, onDisk, machineType)

  System.gc()
  val (ans4b, t8) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id4).view.mapValues { grp =>
      (grp.map(_.v1).sumSIMD.toDouble / grp.size,
       grp.map(_.v2).sumSIMD.toDouble / grp.size,
       grp.map(_.v3).sumSIMD / grp.size)
    }.toMap
  }
  println(s"(${ans4b.size}, 4)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk8, chkt8) = BenchmarkHelpers.timeIt {
    Seq(ans4b.values.map(_._1).sum, ans4b.values.map(_._2).sum, ans4b.values.map(_._3).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans4b.size.toLong, 4,
    solution, ver, git, fun, 2, t8, m, cache, BenchmarkHelpers.makeChk(chk8), chkt8, onDisk, machineType)
  println(ans4b.take(3))
  System.out.flush()

  // Question 5: sum v1:v3 by id6
  question = "sum v1:v3 by id6"
  System.gc()
  val (ans5, t9) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id6).view.mapValues { grp =>
      (grp.map(_.v1.toLong).sumSIMD, grp.map(_.v2.toLong).sumSIMD, grp.map(_.v3).sumSIMD)
    }.toMap
  }
  println(s"(${ans5.size}, 4)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk9, chkt9) = BenchmarkHelpers.timeIt {
    Seq(ans5.values.map(_._1).sum, ans5.values.map(_._2).sum, ans5.values.map(_._3).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans5.size.toLong, 4,
    solution, ver, git, fun, 1, t9, m, cache, BenchmarkHelpers.makeChk(chk9), chkt9, onDisk, machineType)

  System.gc()
  val (ans5b, t10) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id6).view.mapValues { grp =>
      (grp.map(_.v1.toLong).sumSIMD, grp.map(_.v2.toLong).sumSIMD, grp.map(_.v3).sumSIMD)
    }.toMap
  }
  println(s"(${ans5b.size}, 4)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk10, chkt10) = BenchmarkHelpers.timeIt {
    Seq(ans5b.values.map(_._1).sum, ans5b.values.map(_._2).sum, ans5b.values.map(_._3).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans5b.size.toLong, 4,
    solution, ver, git, fun, 2, t10, m, cache, BenchmarkHelpers.makeChk(chk10), chkt10, onDisk, machineType)
  println(ans5b.take(3))
  System.out.flush()

  // Question 6: median v3 sd v3 by id4 id5
  question = "median v3 sd v3 by id4 id5"
  System.gc()
  val (ans6, t11) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id4, r.id5)).view.mapValues { grp =>
      val v3s = grp.map(_.v3).sorted
      val median = if (v3s.isEmpty) 0.0 else {
        val mid = v3s.size / 2
        if (v3s.size % 2 == 0) (v3s(mid-1) + v3s(mid)) / 2.0 else v3s(mid)
      }
      val (mean, variance) = v3s.meanAndVariance
      val sd = math.sqrt(variance)
      (median, sd)
    }.toMap
  }
  println(s"(${ans6.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk11, chkt11) = BenchmarkHelpers.timeIt {
    Seq(ans6.values.map(_._1).sum, ans6.values.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans6.size.toLong, 3,
    solution, ver, git, fun, 1, t11, m, cache, BenchmarkHelpers.makeChk(chk11), chkt11, onDisk, machineType)

  System.gc()
  val (ans6b, t12) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id4, r.id5)).view.mapValues { grp =>
      val v3s = grp.map(_.v3).sorted
      val median = if (v3s.isEmpty) 0.0 else {
        val mid = v3s.size / 2
        if (v3s.size % 2 == 0) (v3s(mid-1) + v3s(mid)) / 2.0 else v3s(mid)
      }
      val (mean, variance) = v3s.meanAndVariance
      val sd = math.sqrt(variance)
      (median, sd)
    }.toMap
  }
  println(s"(${ans6b.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk12, chkt12) = BenchmarkHelpers.timeIt {
    Seq(ans6b.values.map(_._1).sum, ans6b.values.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans6b.size.toLong, 3,
    solution, ver, git, fun, 2, t12, m, cache, BenchmarkHelpers.makeChk(chk12), chkt12, onDisk, machineType)
  println(ans6b.take(3))
  System.out.flush()

  // Question 7: max v1 - min v2 by id3
  question = "max v1 - min v2 by id3"
  System.gc()
  val (ans7, t13) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id3).view.mapValues { grp =>
      grp.map(_.v1).maxSIMD - grp.map(_.v2).minSIMD
    }.toMap
  }
  println(s"(${ans7.size}, 2)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk13, chkt13) = BenchmarkHelpers.timeIt {
    Seq(ans7.values.map(_.toLong).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans7.size.toLong, 2,
    solution, ver, git, fun, 1, t13, m, cache, BenchmarkHelpers.makeChk(chk13), chkt13, onDisk, machineType)

  System.gc()
  val (ans7b, t14) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id3).view.mapValues { grp =>
      grp.map(_.v1).maxSIMD - grp.map(_.v2).minSIMD
    }.toMap
  }
  println(s"(${ans7b.size}, 2)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk14, chkt14) = BenchmarkHelpers.timeIt {
    Seq(ans7b.values.map(_.toLong).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans7b.size.toLong, 2,
    solution, ver, git, fun, 2, t14, m, cache, BenchmarkHelpers.makeChk(chk14), chkt14, onDisk, machineType)
  println(ans7b.take(3))
  System.out.flush()

  // Question 8: largest two v3 by id6
  question = "largest two v3 by id6"
  System.gc()
  val (ans8, t15) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id6).flatMap { case (id, grp) =>
      grp.map(_.v3).sorted.reverse.take(2).map(v => (id, v)).toSeq
    }.toSeq
  }
  println(s"(${ans8.size}, 2)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk15, chkt15) = BenchmarkHelpers.timeIt {
    Seq(ans8.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans8.size.toLong, 2,
    solution, ver, git, fun, 1, t15, m, cache, BenchmarkHelpers.makeChk(chk15), chkt15, onDisk, machineType)

  System.gc()
  val (ans8b, t16) = BenchmarkHelpers.timeIt {
    data.groupBy(_.id6).flatMap { case (id, grp) =>
      grp.map(_.v3).sorted.reverse.take(2).map(v => (id, v)).toSeq
    }.toSeq
  }
  println(s"(${ans8b.size}, 2)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk16, chkt16) = BenchmarkHelpers.timeIt {
    Seq(ans8b.map(_._2).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans8b.size.toLong, 2,
    solution, ver, git, fun, 2, t16, m, cache, BenchmarkHelpers.makeChk(chk16), chkt16, onDisk, machineType)
  println(ans8b.take(3))
  System.out.flush()

  // Question 9: regression v1 v2 by id2 id4
  question = "regression v1 v2 by id2 id4"
  System.gc()
  val (ans9, t17) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id2, r.id4)).view.mapValues { grp =>
      val v1s = grp.map(_.v1.toDouble)
      val v2s = grp.map(_.v2.toDouble)
      val corr = v1s.corr(v2s)
      corr * corr // r-squared
    }.toMap
  }
  println(s"(${ans9.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk17, chkt17) = BenchmarkHelpers.timeIt {
    Seq(ans9.values.sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans9.size.toLong, 3,
    solution, ver, git, fun, 1, t17, m, cache, BenchmarkHelpers.makeChk(chk17), chkt17, onDisk, machineType)

  System.gc()
  val (ans9b, t18) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id2, r.id4)).view.mapValues { grp =>
      val v1s = grp.map(_.v1.toDouble)
      val v2s = grp.map(_.v2.toDouble)
      val corr = v1s.corr(v2s)
      corr * corr
    }.toMap
  }
  println(s"(${ans9b.size}, 3)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk18, chkt18) = BenchmarkHelpers.timeIt {
    Seq(ans9b.values.sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans9b.size.toLong, 3,
    solution, ver, git, fun, 2, t18, m, cache, BenchmarkHelpers.makeChk(chk18), chkt18, onDisk, machineType)
  println(ans9b.take(3))
  System.out.flush()

  // Question 10: sum v3 count by id1:id6
  question = "sum v3 count by id1:id6"
  System.gc()
  val (ans10, t19) = BenchmarkHelpers.timeIt {
    data.groupBy(r => (r.id1, r.id2, r.id3, r.id4, r.id5, r.id6))
      .view.mapValues(grp => (grp.map(_.v3).sumSIMD, grp.size)).toMap
  }
  println(s"(${ans10.size}, 8)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk19, chkt19) = BenchmarkHelpers.timeIt {
    Seq(ans10.values.map(_._1).sum, ans10.values.map(_._2.toLong).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans10.size.toLong, 8,
    solution, ver, git, fun, 1, t19, m, cache, BenchmarkHelpers.makeChk(chk19), chkt19, onDisk, machineType)

  System.gc()
  val (ans10b, t20) = BenchmarkHelpers.timeIt {
    data.groupMapReduce(r => (r.id1, r.id2, r.id3, r.id4, r.id5, r.id6))(r => (r.v3, 1)) { case ((sum1, cnt1), (sum2, cnt2)) => (sum1 + sum2, cnt1 + cnt2) }
  }
  println(s"(${ans10b.size}, 8)")
  System.out.flush()
  m = BenchmarkHelpers.memoryUsage()
  val (chk20, chkt20) = BenchmarkHelpers.timeIt {
    Seq(ans10b.values.map(_._1).sum, ans10b.values.map(_._2.toLong).sum)
  }
  BenchmarkHelpers.writeLog(task, dataName, inRows, question, ans10b.size.toLong, 8,
    solution, ver, git, fun, 2, t20, m, cache, BenchmarkHelpers.makeChk(chk20), chkt20, onDisk, machineType)
  println(ans10b.take(3))
  System.out.flush()

  val totalTime = (System.nanoTime() - taskInit) / 1e9
  println(f"grouping finished, took ${totalTime}%.3fs")
  System.out.flush()
