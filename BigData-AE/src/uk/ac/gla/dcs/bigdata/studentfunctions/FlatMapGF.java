package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class FlatMapGF implements FlatMapGroupsFunction<Query,Document, RankedResult>{

	@Override
	public Iterator<RankedResult> call(Query key, Iterator<Document> values) throws Exception {
		
		return null;
	}



}
