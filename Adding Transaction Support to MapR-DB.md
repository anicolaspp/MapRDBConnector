# Transaction Support for MapR-DB

Sometimes there are limits around how much we can stretch certain technology. In the case of MapR-DB, these limits seem to never get closer while we add more and more capabilities on top it. 

Previously, we have talked about many things we can do using MapR-DB. Make sure you check this posts. 

Today, we want to introduce the idea of `transactional writing` when using MapR-DB. 

This is not something supported out of the box by this distributed database, however, when using Apache Spark, we could implement similar concepts to what relational databases have. 

It is not until recently that Spark added APIs to start supporting these ideas so today we are going to review some of these APIs while proposing a way add `transactions` to MapR-DB.

Certainly, transactional context is, in our case, at the application layer, so there are only a few things we can actually do. Apache Spark propose it as `best effort` since in reality this is a very fragile context and many, many things can go wrong. 

Let's review what Apache Spark API offers in order to support `transactional writes`. 

The most basic build block is called `DataWriter[Row]` and it is defined as follows. 

```scala
class MapRDBDataWriter extends DataWriter[Row] with Logging {
  override def write(record: Row): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???
}

```
A `DataWriter[Row]` is in charge or writing a particular partition of the distributed Spark data to the target source. 


The `write` function receives the individuals records to be written down to our target source, in our case, MapR-DB.

The `commit` function is called once all records of the partition has been successfully written down. 

`about` is then called if any record in the partition fails to be written down. 

Putting in context, in order to a transaction to happen, all partitions must successful commit, but at the partition level it is impossible to know what has happened to other partitions. In other words, the transaction must be processed in two phases. One phase is all partitions commit correctly (task level) and a second phase at the job level. If anything fails, the transaction fails and we must provide a way to ***roll it back***. 

## MapRDBDataWriterFactory

The `MapRDBDataWriterFactory` is in charge of creating multiple `DataWriter`s. This class looks like this.

```scala
class MapRDBDataWriterFactory(table: String, schema: StructType) extends DataWriterFactory[Row] {
	override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = new MapRDBDataWriter(...)
}
```

It is important to notice that Spark might call `createDataWriter` many times for the same `partitionId`. This happens if a particular task is slow or the task fails, Spark creates a new `DataWriter` with a different `attemptNumber` which implies that many writers might be writing the same data. 

Our implementation looks like this.

```scala
cclass MapRDBDataWriterFactory(table: String, schema: StructType) extends DataWriterFactory[Row] {

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

 private val writtenIds = scala.collection.mutable.ListBuffer.empty[String]
  
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = new DataWriter[Row] with Logging {
    
    log.info(s"PROCESSING PARTITION ID: $partitionId ; ATTEMPT: $attemptNumber")

    override def write(record: Row): Unit = {

      val doc = schema
        .fields
        .map(field => (field.name, schema.fieldIndex(field.name)))
        .foldLeft(connection.newDocumentBuilder()) { case (acc, (name, idx)) => acc.put(name, record.getString(idx)) }
        .getDocument

      this.synchronized {
        if (!writtenIds.contains(doc.getIdString)) {
          store.insert(doc)
          writtenIds.append(doc.getIdString)
        }
      }
    }

    override def commit(): WriterCommitMessage = {
      log.info(s"PARTITION $partitionId COMMITTED AFTER ATTEMPT $attemptNumber")

      CommittedIds(partitionId, writtenIds.toSet)
    }

    override def abort(): Unit = {
      log.info(s"PARTITION $partitionId ABORTED AFTER ATTEMPT $attemptNumber")

      MapRDBCleaner.clean(writtenIds.toSet, table)

      log.info(s"PARTITION $partitionId CLEANED UP")
    }
  }
}
```

Notice the in the `write` function, given a `Row` and the corresponding `Schema`, we can build an `OJAI` Documents and insert it to MapR-DB. Then we save the `_id`s so we can rollback the data written in this partition if something goes wrong. 

We have optimized it a little bit, so records are not written twice by having a shared state with the `_id`s of already written data. 

The `abort` function does exactly what we just described. If it is called, it deletes the already written records (rollback). 

`commit` informs that all records for the partition has been written back to the driver. 

## MapRDBDataSourceWriter

The `MapRDBDataSourceWriter` runs at the driver and it is in charge of collecting and controlling the results of each partition. 

```scala

class MapRDBDataSourceWriter(table: String, schema: StructType) extends DataSourceWriter with Logging {

	override def createWriterFactory(): DataWriterFactory[Row] = ???
	
	override def commit(messages: Array[WriterCommitMessage]): Unit = ???
	
	override def abort(messages: Array[WriterCommitMessage]): Unit = ???

}
```

The `createWriterFactory` just creates the `DataWriterFactory` that runs on the executor side. 

```scala
override def createWriterFactory(): DataWriterFactory[Row] = new MapRDBDataWriterFactory(table, schema)

```

`committs` gets the commit messages from each of the `DataWriter`. Each all `commits` are successful then the entire job is successful and we are good to go, the transaction has finished. 

The there is a least one partition which was not successful committed, the job has failed. The failed partition knows how to rollback itself (explained above) but the driver must roll back any other data at this point since partitions don't see each other. In order to do this we collect all successfully committed `_id`s from all committed partitions. 

```scala
override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val ids = messages.foldLeft(Set.empty[String]) { case (acc, CommittedIds(partitionId, partitionIds)) =>
      log.info(s"PARTITION $partitionId HAS BEEN CONFIRMED BY DRIVER")

      acc ++ partitionIds
    }

    // Let's make sure this is thread-safe
    globallyCommittedIds = this.synchronized {
      globallyCommittedIds ++ ids
    }
  }
```

If a partition fails, then `abort` in the `DataWriter` is called (explained above) so partition data is rolled back. The `abort` in the driver is called so we roll back any other written data. 

```
 override def abort(messages: Array[WriterCommitMessage]): Unit = {
    log.info("JOB BEING ABORTED")
    log.info("JOB CLEANING UP")

    MapRDBCleaner.clean(globallyCommittedIds.toSet, table)
  }
```

## WriteSupport 

`WriteSupport` is the entry point for injecting our code into Spark.

```scala
class Writer extends WriteSupport with Logging {

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    val tablePath = options.get("path").get()

    log.info(s"TABLE PATH BEING USED: $tablePath")

    java.util.Optional.of(new MapRDBDataSourceWriter(tablePath, schema))
  }
}
```

## Using Transactions Writes

```scala
val df: DataFrame = ...

df.write
  .format("com.github.anicolaspp.spark.sql.writing.Writer")
  .save(path)
```

Of course we are wrapping this into a better API so we can do the following.

```scala
data.saveToMapRDB("/user/mapr/tables/my_table", withTransaction = true)
```

By indicating `withTransaction = true` Spark tries it best to write the given `DataFrame` in transactional mode using the described mechanics above. If `withTransaction = false` then we use the regular, official MapR-DB Connector for Apache Spark to write the `DataFrame` down. 

## The MapRDBConnector Code

The described code is part of our [MapRDBConnector](https://github.com/anicolaspp/MapRDBConnector) but belongs to a different branch for now [Transaction Support](https://github.com/anicolaspp/MapRDBConnector/tree/transactional-writer-support).

## Disclaimers

- This is still in experimental phase and is not being include just yet in our [MapRDBConnector](https://github.com/anicolaspp/MapRDBConnector) releases. It can be used if you get the code and compile it from source.
- If the transaction failed because the Spark job is interrupted, we don't have a way to rollback the already written data. Our goal is to hide this data from the user, but we are still researching this. For now, you must manually clean it up.

