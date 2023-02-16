package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ContentFilterFlatMap implements FlatMapFunction<NewsArticle,NewsArticle>{

	
	@Override
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

}
