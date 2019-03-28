object News {

  class Article(title: String, content: String) {

    override def toString: String = {
      s"title:${title} content:${content}"
    }
  }

  def main(args: Array[String]): Unit = {
    val article = new Article("this is title", "hi,this is content")
    println(article)
  }

}
