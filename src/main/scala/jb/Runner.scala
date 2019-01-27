package jb

import jb.norm.Normalizer.normalizeAll
import jb.server.SparkEmbedded

object Runner {

  def main(args: Array[String]): Unit = {
    SparkEmbedded.setLogWarn()
    normalizeAll("A", "B")
  }

}
