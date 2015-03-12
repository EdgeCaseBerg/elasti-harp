import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._

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
	implicit val client = ElasticClient.local

	client.execute {
  		create index indexAndType.dropRight(indexAndType.indexOf("/")) mappings (
  			indexAndType.drop(indexAndType.indexOf("/") + 1) as (
  				"title" typed StringType,
    			"description" typed StringType,
    			"keywords" typed StringType
    		)
    	)
	}

	def sendObjToElasticSearch(indexAndType : String, id : String, obj: Map[String,String])(implicit client: ElasticClient) {
		client.execute {
			index into s"$indexAndType/$id" fields obj
		}
	}

	for( (key: String, obj: Map[_,_]) <- jsonMap) {
		/* ignore _ directories, you may need to edit asInstanceOf if you have a more complicated object */
		if (!key.startsWith("_",0)) sendObjToElasticSearch(indexAndType, key, obj.asInstanceOf[Map[String,String]])
	}

}