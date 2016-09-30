import java.io.{InputStream, OutputStream}

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.google.gson.Gson

import scala.io.Source

/**
  * Created by menzelmi on 30/09/16.
  */
class StockTradeAPI extends RequestStreamHandler {

  private val STATS_TARGET_TABLE: String = "StockTradeStats"

  def handleRequest(input: InputStream, output: OutputStream, context: Context) = {

    Source.fromInputStream(input, "utf-8").getLines().foreach(println)

    lazy val db: AmazonDynamoDB = new AmazonDynamoDBClient().withRegion(Regions.EU_WEST_1)

    output.write(
      new Gson().toJson(
        db.scan(new ScanRequest().withTableName(STATS_TARGET_TABLE)).getItems
      ).getBytes()
    )
  }

}
