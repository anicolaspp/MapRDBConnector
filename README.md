# MapRDBConnector
An independent MapR-DB Connector for Apache Spark that fully utilizes MapR-DB secondary indexes.

The main idea behind implementing a new **MapR-DB Connector for Apache Spark** is to overcome the current limitations of the official connector. 

## MapR-DB Secondary Indexes

MapR-DB automatically indexes the field `_id` and we can manually add many more indexes that can help to speed up queries that use the defined indexes. **Apache Drill**, for instance, makes use of the defined indexes for the tables being queried so the underlying computations are greatly speeded up. 

When using **Apache Spark** we are expecting that the same as in **Apache Drill** applies. However, The official **MapR-DB Connector for Apache Spark** does not use the secondary indexes defined on the tables. The official connector implements filters and projections push down to reduce the amount of data being transfer beetwen **MapR-DB** and **Apache Spark**, still, secondary indexes are just not used. 

Our implementation mimics the same API as the official connector. It also supports filters and projections push down and *it adds the ability to use secondary indexes* if appropiated.

## Using Our MapRDBConnector

The following snippet show an example using our **MapRDBConnector**. Given a schema we can load data from a MapR-DB table. The interesting part in here is that the fields `uid` is a secondary index in the table `/user/mapr/tables/data`. 

```scala 
import com.github.anicolaspp.spark.sql.MapRDB._

val schema = StructType(Seq(StructField("_id", StringType), StructField("first_name", StringType), StructField("uid", StringType)))

    sparkSession
      .loadFromMapRDB("/user/mapr/tables/data", schema)
      .filter("uid = '101'")
      .select("_id", "first_name")
      .show()
```      

When running the code above, our **MapRDBConnector** uses the corresponding MapR-DB secondary index. We can examine the output of the underlyign OJAI object to make sure that, in fact, it uses the secondary index. Notice the `"indexName":"uid_idx"` which indicates that the index `uid` is being used when running the query. 


```json
QUERY PLAN: {"QueryPlan":[
  [{
    "streamName":"DBDocumentStream",
    "parameters":{
      "queryConditionPath":false,
      "indexName":"uid_idx",
      "projectionPath":[
        "uid",
        "_id"
      ],
      "primaryTable":"/user/mapr/tables/data"
    }
  }
  ]
]}
```

### Giving Index Hints

Sometimes we know the filter(s) involved in the query and it will be good if we could help OJAI a little bit by given hints about the known index(es). 

The following code shows how to add index hints

```scala
 sparkSession
    .loadFromMapRDB("/user/mapr/tables/from_parquet", schema, "idx_2")
    .filter("_2 = 'n2078258460719121947'")
    .show()
```

## Projections and Filters Push Down

Our **MapRDBConnector** is able to push every projection down. In other worlds, if we run the following query, our **MapRDBConnector** makes sure that only the projected columns are read from MapR-DB reducing the amount of data being transferred. 

Only `_id` and `first_name` are extracted from MapR-DB. The field `uid` is only used for the filter, but its value is not transfer between MapR-DB and Spark.

```scala
val schema = StructType(Seq(StructField("_id", StringType), StructField("first_name", StringType), StructField("uid", StringType)))

    sparkSession
      .loadFromMapRDB("/user/mapr/tables/data", schema)
      .filter("uid = '101'")
      .select("_id", "first_name")
      .show()
```

In the same way we can push filters down to MapR-DB. It is import to notice (our main feature) that is the column being used for the filter is a secondary index, then it will be used to narrow in a very performant way the rows required. 

## Reading Parallelism and Data Locality

Our **MapRDBConnector** is able to read the Table information (thanks to @iulianov) and it lauches a task for each Table region.

In addition to this, our **MapRDBConnector** hints Spark so that Spark puts the reading task as close as possible to where the corresponding Table region lives in the cluster. In other words, if `region 1` lives in node `10.20.30.40`, our library passes this information to Spark so that when Spark launches the reading task for `region 1` it puts it on an executor running on the same node `10.20.30.40`. This is up to Spark and the resources availability, but we provide all information Spark needs to sucessfully maintain data locatily. 

