package com.abmodi.chingari.repl

import scala.tools.nsc.GenericRunnerSettings

object Main {
  private def scalaOptionError(msg: String): Unit = {
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }
  def main(args: Array[String]): Unit = {
    val settings = new GenericRunnerSettings(scalaOptionError)
    new ChingariILoop().process(settings)
  }
}
