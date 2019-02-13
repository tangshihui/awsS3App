import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest

object App {


  def listObjectsOf(bucketName: String): Unit = {
    val awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)
    var s3Client = new AmazonS3Client(awsCredentials)

    var region = Region.getRegion(Regions.US_WEST_2)
    s3Client.setRegion(region)

    var objects = s3Client.listObjects(bucketName)

    do {
      objects.getObjectSummaries.forEach(s => println("key:" + s.getKey))
    } while (objects.isTruncated)
  }

  def readTxt(bucketName: String, keyName: String): Unit = {
    val awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)
    var s3Client = new AmazonS3Client(awsCredentials)

    var region = Region.getRegion(Regions.US_WEST_2)
    s3Client.setRegion(region)

    var s3Object = s3Client.getObject(new GetObjectRequest(bucketName, keyName))

    println("content-type:" + s3Object.getObjectMetadata.getContentType)

    //read inputstream to string
    val lines: Iterator[String] = scala.io.Source.fromInputStream(s3Object.getObjectContent, "utf-8").getLines()
    if (lines != null) {
      lines.foreach(l => println(l))
    }


  }

  val ACCESS_KEY = sys.env("AWS_ACCESS_KEY")
  val SECRET_KEY = sys.env("AWS_SECRET_KEY")

  def main(args: Array[String]): Unit = {

    val BUCKET_NAME = "gridx-oregon-pge-dev"

    //println(ACCESS_KEY)
    //listObjectsOf(bucketName = BUCKET_NAME)


    val keyName = "jing/pge/2018-12-10/pgedetailrequest.txt"

    readTxt(bucketName = BUCKET_NAME, keyName)


  }


}
