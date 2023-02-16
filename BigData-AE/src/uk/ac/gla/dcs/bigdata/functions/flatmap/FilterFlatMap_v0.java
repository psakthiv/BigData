package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterFlatMap_v0 implements MapFunction<NewsArticle,List<String>>{
		
	private static final long serialVersionUID = 41553077168611092L;

	@Override
	public List<String> call(NewsArticle newsArticle) throws Exception {
		
		TextPreProcessor preProcessor = new TextPreProcessor();
		
		List<String> tokenizedNewsList = new ArrayList<String>();
		
		newsArticle.getContents().forEach(content -> {
			if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
				
				tokenizedNewsList.addAll(preProcessor.process(content.getSubtype().toLowerCase()));
			}
		});
		
		tokenizedNewsList.addAll(preProcessor.process(newsArticle.getTitle().toLowerCase()));
		
		return tokenizedNewsList;
	}

}
