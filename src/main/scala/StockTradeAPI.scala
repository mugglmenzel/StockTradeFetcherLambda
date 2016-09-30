import java.io.{InputStream, OutputStream}

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{DescribeStreamRequest, GetRecordsRequest, GetShardIteratorRequest, ShardIteratorType}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient, AmazonDynamoDBStreams, AmazonDynamoDBStreamsClient}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.google.gson.Gson

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Created by menzelmi on 30/09/16.
  */
class StockTradeAPI extends RequestStreamHandler {

  private val STATS_SOURCE_TABLE: String = "StockTradeStats"

  def handleRequest(input: InputStream, output: OutputStream, context: Context) = {

    Source.fromInputStream(input, "utf-8").getLines().foreach(println)

    lazy val db: AmazonDynamoDB = new AmazonDynamoDBClient().withRegion(Regions.EU_WEST_1)
    lazy val dbs: AmazonDynamoDBStreams = new AmazonDynamoDBStreamsClient().withRegion(Regions.EU_WEST_1)

    lazy val tab = db.describeTable(STATS_SOURCE_TABLE).getTable

    val records = dbs.describeStream(
      new DescribeStreamRequest().withStreamArn(tab.getLatestStreamArn)
    ).getStreamDescription.getShards.toList.flatMap { shard =>
      println(s"processing ${shard.getShardId}")
      dbs.getRecords(
        new GetRecordsRequest()
          .withShardIterator(
            dbs.getShardIterator(
              new GetShardIteratorRequest()
                .withStreamArn(tab.getLatestStreamArn)
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .withShardId(shard.getShardId)
            ).getShardIterator
          ).withLimit(100)
      ).getRecords.toList.map { record =>
        println(s"adding record ${record.getDynamodb.getSequenceNumber}")
        record.getDynamodb.getNewImage.values().toSeq.map{v =>
          println(s"extracting value $v")
          Option(v.getN).getOrElse(v.getS)
        }.asJavaCollection
      }.asJavaCollection
    }.asJavaCollection

    println(s"compute $records")

    output.write(new Gson().toJson(records).getBytes())
  }

}
