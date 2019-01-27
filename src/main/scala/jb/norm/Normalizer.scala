package jb.norm

import java.io.File
import java.nio.file.{Files, Paths}

import jb.server.SparkEmbedded
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{col, max, min}

object Normalizer {

  val functions = Array("min", "max")
  val prefix = "_c"

  def getAsDouble(value: Any): Double = {
    value match {
      case value: Double =>
        value
      case value: Int =>
        value.toDouble
    }
  }

  def normalize(file: File, to: String): Unit = {
    val df = SparkEmbedded.ss.read.format("csv").option("inferSchema", "true").load(file.getAbsolutePath)
    val cols = df.columns.slice(0, df.columns.length - 1)
    val stats = cols.flatMap(i => Array(min(i), max(i)))
    val row = df.select(stats: _*).head
    var colsNorm = cols.indices
      .map(i => mapToCol(cols, row, i))
    colsNorm :+= col(df.columns.last)
    df.coalesce(1)
      .select(colsNorm: _*)
      .write
      .format("csv")
      .save(to + "/" + file.getName + "tmp")
    val sourceDir = new File(to + "/" + file.getName + "tmp")
    val source = sourceDir.listFiles().filter(_.getName.endsWith(".csv")).head
    Files.move(source.toPath, Paths.get(to + "/" + file.getName))
  }

  private def mapToCol(cols: Array[String], row: Row, i: Int): Column = {
    if (!row.get(2 * i).equals(row.get(2 * i + 1)))
      (col(cols(i)) - getAsDouble(row.get(2 * i))) / (getAsDouble(row.get(2 * i + 1)) - getAsDouble(row.get(2 * i)))
    else
      col(cols(i))
  }

  def normalizeAll(from: String, to: String): Unit = {
    val dirFrom = new File(from)
    val dirTo = new File(to)
    if (dirFrom.isFile || dirTo.isFile) throw new RuntimeException("Must be directories")
    dirFrom.listFiles.foreach(file => normalize(file, to))
    SparkEmbedded.ss.close()
    new File(to).listFiles().filter(_.isDirectory).foreach(_.delete())
  }

}
