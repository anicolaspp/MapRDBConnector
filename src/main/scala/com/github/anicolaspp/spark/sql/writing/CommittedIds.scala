package com.github.anicolaspp.spark.sql.writing

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

case class CommittedIds(partitionId: Int, ids: Set[String]) extends WriterCommitMessage
