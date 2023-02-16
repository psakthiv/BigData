package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ContentFilterReduce implements ReduceFunction<NewsArticle>{

	
	//@Override
	public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {

		List<NewsArticle> finalArticles = new ArrayList<NewsArticle>();
		List<ContentItem> finalContents = new ArrayList<ContentItem>();
		
		newsArticle.getContents().forEach(content -> {
			if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
				if(finalContents.size() < 5) {
					finalContents.add(content);
					newsArticle.setContents(finalContents);
					
				}
				
			}
		});
		
		finalArticles.add(newsArticle);
		
		return finalArticles.iterator();
	}

	@Override
	public NewsArticle call(NewsArticle finalArticles, NewsArticle newsArticle) throws Exception {
		//List<NewsArticle> finalArticles = new ArrayList<NewsArticle>();
		List<ContentItem> finalContents = new ArrayList<ContentItem>();
		
		finalArticles.setContents(finalContents);
		newsArticle.getContents().forEach(content -> {
			
			if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
				if(finalContents.size() < 5) {
					finalContents.add(content);
					finalArticles.setContents(finalContents);
					
				}
				
			}
		});
		
		//finalArticles.add(newsArticle);
		
		return finalArticles;
	}

}
