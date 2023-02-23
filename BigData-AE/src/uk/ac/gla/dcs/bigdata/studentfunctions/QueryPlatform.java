package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class QueryPlatform implements MapFunction<Document,Query>{

	@Override
	public Query call(Document value) throws Exception {
		return null;
	}

}
