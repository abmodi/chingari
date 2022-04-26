package com.abmodi.chingari.sql

import com.abmodi.chingari.core.DataFrame
import com.abmodi.chingari.logical._
import org.antlr.v4.runtime.{CharStream, CharStreams, CodePointCharStream, CommonTokenStream, IntStream}
import org.antlr.v4.runtime.misc.Interval


private class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

case object SParser {
  def apply(sql : String, tables: Map[String, DataFrame]) : LogicalPlan = {
    val lexer = new SqlLexer(new UpperCaseCharStream(CharStreams.fromString(sql)))
    val tokenStream = new CommonTokenStream(lexer);
    val parser = new SqlParser(tokenStream);
    val visitor = new AstBuilder(tables)
    visitor.visitSingleStatement(parser.singleStatement())
  }
}
