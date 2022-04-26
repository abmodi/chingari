package com.abmodi.chingari.repl

import java.io.BufferedReader
import scala.tools.nsc.interpreter._
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class ChingariILoop(in0: Option[BufferedReader], out: JPrintWriter)
  extends ILoop(in0, out) {
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  val initializationCommands: Seq[String] = Seq(
    "import com.abmodi.chingari.core._",
    "import com.abmodi.chingari.datasource._",
    "import com.abmodi.chingari.logical._",
    "import com.abmodi.chingari.physical._",
    "@transient val cc = Context()"
  )

  def initializeChingari() : Unit = {
    savingReplayStack {
      initializationCommands.foreach(intp quietRun _)
    }
  }

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeChingari()
    echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
  }

  override def replay(): Unit = {
    initializeChingari()
    super.replay()
  }

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    initializeChingari()
  }

  /** Print a welcome message */
  override def printWelcome(): Unit = {
    echo("""Welcome to

 ,-----.,--.     ,--.                              ,--.
'  .--./|  ,---. `--',--,--,  ,---.  ,--,--.,--.--.`--'
|  |    |  .-.  |,--.|      \| .-. |' ,-.  ||  .--',--.
'  '--'\|  | |  ||  ||  ||  |' '-' '\ '-'  ||  |   |  |
 `-----'`--' `--'`--'`--''--'.`-  /  `--`--'`--'   `--'
                             `---'
         """)
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }
}

