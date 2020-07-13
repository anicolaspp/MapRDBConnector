package com.github.anicolaspp.spark

import org.apache.hadoop.fs.PathFilter
import org.ojai.store.DriverManager

object FileOps {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, Path}

  lazy val fs: FileSystem = {
    val conf = new Configuration()

    println(s"File System Configuration: $conf")

    FileSystem.get(conf)
  }

  lazy val dbPathFilter: PathFilter = new PathFilter {
    private lazy val connection = DriverManager.getConnection("ojai:mapr:")

    override def accept(path: Path): Boolean = connection.storeExists(path.toString)
  }
}
