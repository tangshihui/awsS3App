import java.io.File

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._

object App {

  def getS3Client(): AmazonS3Client = {
    var s3Client = new AmazonS3Client()
    var region = Region.getRegion(Regions.US_WEST_2)
    s3Client.setRegion(region)
    s3Client
  }


  def listObjectsOf(bucketName: String): Unit = {
    var s3Client = getS3Client()

    var objects = s3Client.listObjects(bucketName)

    do {
      objects.getObjectSummaries.forEach(s => println("key:" + s.getKey))
    } while (objects.isTruncated)
  }

  def readTxt(bucketName: String, keyName: String): Unit = {
    var s3Client = getS3Client()

    var s3Object = s3Client.getObject(new GetObjectRequest(bucketName, keyName))

    println("content-type:" + s3Object.getObjectMetadata.getContentType)

    //read inputstream to string
    val lines: Iterator[String] = scala.io.Source.fromInputStream(s3Object.getObjectContent, "utf-8").getLines()
    if (lines != null) {
      lines.foreach(l => println(l))
    }
  }

  def putFile(buchetName: String, keyName: String, path: String): Unit = {
    var s3Client = getS3Client()
    var putResult: PutObjectResult = null
    try {
      var file: File = new File(path)
      putResult = s3Client.putObject(new PutObjectRequest(buchetName, keyName, file))
      println("上传文件成功：res:" + putResult)
    } catch {
      case serviceEx: AmazonServiceException => println(serviceEx)
      case clientEx: AmazonClientException => println(clientEx)
      case s3Ex: AmazonS3Exception => println("上传失败：" + s3Ex.getMessage)
      case _ => println("Error:" + _)
    }

  }


  //val ACCESS_KEY = sys.env("AWS_ACCESS_KEY")
  //val SECRET_KEY = sys.env("AWS_SECRET_KEY")

  def main(args: Array[String]): Unit = {

    val BUCKET_NAME = "yum-repo-uw2"

    //println(ACCESS_KEY)
    //listObjectsOf(bucketName = BUCKET_NAME)
    //putFile(BUCKET_NAME, "aws-secret-key/shihui/test.sh", "/Users/tangshihui/Learn/shell/users")



    val keyName = "jing/pge/2018-12-10/pgedetailrequest.txt"

    readTxt(bucketName = BUCKET_NAME, keyName)

    /*println(caseAge(18))
    println(caseAge(25))
    println(caseAge(60))*/


  }

  def caseAge(age: Int): String = {
    age match {
      case 18 => "adult"
      case 60 => "older"
      case _ => "unknown"
    }
  }


}
