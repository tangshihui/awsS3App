import java.io.File
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.Transfer.TransferState
import com.amazonaws.services.s3.transfer.TransferManager
import scala.collection.JavaConverters._
import scala.io.Source


object S3Utils {

  private var client: AmazonS3Client = _

  def getS3Client(): AmazonS3Client = synchronized {
    if (client == null) {
      client = new AmazonS3Client()
      val region = Region.getRegion(Regions.US_WEST_2)
      client.setRegion(region)
    }

    client
  }


  /**
    * List keys of bucket
    *
    * @param bucket
    * @return
    */
  def listKeysOf(bucket: String): List[String] = {
    val s3Client = getS3Client()
    val o = s3Client.listObjects(bucket)
    o.getObjectSummaries.asScala.map(_.getKey).toList
  }


  /**
    * Upload local file to s3
    *
    * @param buchet
    * @param key
    * @param localPath
    * @return s3Path
    */
  def uploadFile(bucket: String, key: String, localPath: String): String = {
    val client = getS3Client()
    val uploader = new TransferManager(client).upload(bucket, key, new File(localPath))

    while (!uploader.isDone()) {
      if (true) {
        println(
          s"""Transfer: ${uploader.getDescription()}
             | - State: ${uploader.getState()}
             | - Progress: ${uploader.getProgress().getBytesTransferred()}""".stripMargin)
      }
      Thread.sleep(500)
    }

    if (uploader.getState() == TransferState.Completed) {
      return s"s3://$bucket/$key"
    } else {
      return null
    }
  }


  /**
    * Read lines of file
    *
    * @param bucket
    * @param key
    * @return
    */
  def readLines(bucket: String, key: String): List[String] = {
    Source.fromInputStream(getS3Client().getObject(bucket, key).getObjectContent).getLines().toList
  }

}