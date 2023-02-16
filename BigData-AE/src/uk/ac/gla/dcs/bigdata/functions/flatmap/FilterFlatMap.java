package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;

import uk.ac.gla.dcs.bigdata.providedstructures.ForDPHCalculation;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterFlatMap implements FlatMapFunction<NewsArticle,ForDPHCalculation>{
		
	private static final long serialVersionUID = 41553077168611092L;

	Dataset<Query> queries;
	
	public FilterFlatMap(Dataset<Query> queries) {
		this.queries = queries;
	}
	
	
	@Override
	public Iterator<ForDPHCalculation> call(NewsArticle newsArticle) throws Exception {
		
		TextPreProcessor preProcessor = new TextPreProcessor();
		List<ForDPHCalculation> forDPHCalculation = new ArrayList<ForDPHCalculation>();
		
		List<String> tokenizedNewsList = new ArrayList<String>();
		
		newsArticle.getContents().forEach(content -> {
			if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
				
				tokenizedNewsList.addAll(preProcessor.process(content.getSubtype().toLowerCase()));
			}
		});
		
		tokenizedNewsList.addAll(preProcessor.process(newsArticle.getTitle().toLowerCase()));
		tokenizedNewsList.add(0, newsArticle.getId());
		
		ForDPHCalculation forDPH = new ForDPHCalculation();
		forDPH.setNoOfOccurances(0);
		
		queries.collectAsList().forEach(query -> {
			query.getQueryTerms().forEach(queryTerm -> {
				tokenizedNewsList.forEach(tokenizedNews -> {
					
					if(tokenizedNews.equals(queryTerm)) {
						forDPH.setDocumentID(tokenizedNewsList.get(0));
						forDPH.setNoOfOccurances(forDPH.getNoOfOccurances() + 1);
						forDPH.setQueryTerm(queryTerm);
					}
				});
			});
		});
		
		
		
		return forDPHCalculation.iterator();
	}

}
