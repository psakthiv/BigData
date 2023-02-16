package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;


public class QueryFilterFlatMap implements FlatMapFunction<Map<NewsArticle, Query> ,String>{

	@Override
	public Iterator<String> call(Map t) throws Exception {
		// TODO Auto-generated method stub
		List<NewsArticle> newsArticles = new ArrayList<NewsArticle>();
		List<Query> queries = new ArrayList<Query>();
		
		NewsArticle finalArticle = new NewsArticle();
		Query finalQuery = new Query();
		
		t.forEach((key,value) ->{
			newsArticles.add((NewsArticle) key);
			
		});
		return null;
	}

}
