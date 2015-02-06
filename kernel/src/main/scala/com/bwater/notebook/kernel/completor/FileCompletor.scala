package com.bwater.notebook.kernel.completor

import java.io.File

import com.bwater.notebook.{Match, StringCompletor}
import org.apache.commons.io.FileUtils

/**
 * Auto-completes file paths based on the current directory
 */
class FileCompletor extends StringCompletor {
  def complete(stringToComplete: String): (String, Seq[Match]) = {
    val filesInCurrentDirectory = listFiles

    val matches = for {
      candidateFile <- filesInCurrentDirectory if normalize(candidateFile).startsWith(normalize(stringToComplete))
    } yield {
      val f = new File(candidateFile)

      val metadata = Map("Path" -> f.getAbsolutePath)
      val extraMetadata = if (f.isDirectory) { Map ("Type" -> "Directory") } else { Map("Size" -> FileUtils.byteCountToDisplaySize(f.length())) }

      Match(candidateFile.toString, metadata ++ extraMetadata)
    }

    (stringToComplete, matches)
  }

  private def normalize(x: String) = x.trim.toLowerCase

  protected def listFiles: Seq[String] = {
    val currentDirectory = new File(".")
    currentDirectory.list.toSeq
  }
}
