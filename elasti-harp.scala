import scalaj.http._
import scala.util.parsing.json._

/* Run me from sbt! eg: sbt _data.json myindex/type */
object elastiharp extends App {
	println("Checking parameters...")
	if (args.length != 2) {
		println("Please pass the _data.json file to read as an argument to the script and the index/type as the second!")
		throw new scala.runtime.StopException()
	}
	val indexAndType = args(1)

	val jsonString = scala.io.Source.fromFile(args(0)).getLines.mkString
	val jsonMap = JSON.parseFull(jsonString).get.asInstanceOf[Map[String,Any]]

	def sendObjToElasticSearch(indexTypeId: String, obj: JSONObject) {
		println(s"Trying to send $indexTypeId")
		val authResult :HttpResponse[String] = Http(s"http://localhost:9200/$indexTypeId").postData(obj.toString())
			.header("Content-Type", "application/json")
			.option(HttpOptions.readTimeout(10000))
			.asString

		if (authResult.code == 200) println(s"Successfully Sent: $indexTypeId")
		else println(s"Failed to send: $indexTypeId")
	}

	for( (key: String, obj: Map[_,_]) <- jsonMap) {
		/* ignore _ directories, you may need to edit asInstanceOf if you have a more complicated object */
		if (!key.startsWith("_",0)) sendObjToElasticSearch(s"$indexAndType/$key", scala.util.parsing.json.JSONObject(obj.asInstanceOf[Map[String,String]]))
	}

}