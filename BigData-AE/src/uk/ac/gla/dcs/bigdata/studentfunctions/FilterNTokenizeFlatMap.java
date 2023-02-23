package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTFPair;

public class FilterNTokenizeFlatMap implements FlatMapFunction<NewsArticle,Document>{

	private List<Query> queries;
	CollectionAccumulator<HashMap> totalFrequencyInCorpus;
	LongAccumulator documentLengthInCorpus;
	
	public FilterNTokenizeFlatMap (List<Query> queries, CollectionAccumulator<HashMap> totalFrequencyInCorpus, LongAccumulator documentLengthInCorpus) {
		this.queries = queries;
		this.totalFrequencyInCorpus = totalFrequencyInCorpus;
		this.documentLengthInCorpus = documentLengthInCorpus;
	}
	
	
	//termFrequencyInCurrentDocument // The number of times the query appears in the document
	//totalTermFrequencyInCorpus // the number of times the query appears in all documents
	//currentDocumentLength // the length of the current document (number of terms in the document)
	//averageDocumentLengthInCorpus // the average length across all documents
	//totalDocsInCorpus // the number of documents in the corpus
	
	
	@Override
	public Iterator<Document> call(NewsArticle newsArticle) throws Exception {
		
		System.out.println("Inside the call function..");
		List<ContentItem> finalContents = new ArrayList<ContentItem>();
		
		TextPreProcessor preProcessor = new TextPreProcessor();
		
		List<Document> documentList = new ArrayList<Document>();
		
		Document document = new Document();
		
		List<String> stringList = new ArrayList<String>();
	
			if(Objects.nonNull(newsArticle.getTitle())) {
				System.out.println("The title is not null..");
			newsArticle.getContents().forEach(content -> {
				System.out.println();
				if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
					if(finalContents.size() < 5) {
						finalContents.add(content);
						newsArticle.setContents(finalContents);
						stringList.addAll(preProcessor.process(content.getContent().toLowerCase()));
					}
					
				}
			});
			document.setTokenizedContents(stringList);
			document.setNewsArticle(newsArticle);
			document.setCurrentDocumentLength(document.getTokenizedContents().size());
			
			
			stringList.addAll(preProcessor.process(newsArticle.getTitle().toLowerCase()));
			List<String> tokens = document.getTokenizedContents();
			HashMap<Query, Integer> queryCount = new HashMap<Query, Integer>();
			
			queries.stream().forEach(query ->{
				List<String> queryTerms = query.getQueryTerms();
				queryTerms.stream().forEach(queryTerm -> {
					tokens.stream().forEach(token -> {
						if(token.toLowerCase().toString().equals(queryTerm.toLowerCase().toString())) {
							if(Objects.isNull(document.getNoOfOccurances())) {
								List<QueryTFPair> queryPairList = new ArrayList<QueryTFPair>();
								QueryTFPair queryPair = new QueryTFPair();
								queryPair.setQuery(query);
								queryPair.setTermFrequency(1);
								queryPairList.add(queryPair);
								
								document.setNoOfOccurances(queryPairList);
	
							}
							else {
								boolean matchFound = false;
								for(QueryTFPair queryTFPair: document.getNoOfOccurances()){
									if(queryTFPair.getQuery().getOriginalQuery().equals(query.getOriginalQuery())) {
										queryTFPair.setTermFrequency(queryTFPair.getTermFrequency() + 1);
										matchFound = true;
										break;
									}
								}
								if(!matchFound) {
									QueryTFPair queryPair = new QueryTFPair();
									queryPair.setQuery(query);
									queryPair.setTermFrequency(1);
									
									document.getNoOfOccurances().add(queryPair);
								
									
									
								}
								
							}
							
						}
					});
					
				});
				
			});
			Gson gson = new Gson();
			List<HashMap> hashMapList = totalFrequencyInCorpus.value();
			if(Objects.nonNull(document.getNoOfOccurances()) && document.getNoOfOccurances().size() > 0) {
				document.getNoOfOccurances().forEach(queryTFPair ->{
					if(Objects.nonNull(hashMapList) && hashMapList.size() > 0) {
						hashMapList.forEach(hashMap ->{
							if(Objects.nonNull(hashMap.get(queryTFPair.getQuery()))) {
								int currentValue = (int)hashMap.get(queryTFPair.getQuery());
								hashMap.replace(queryTFPair.getQuery(), currentValue+queryTFPair.getTermFrequency());
							}
						});
					}
					else {
	//					System.out.println("*****Here in the getNoOfOccurances*****");
		//				System.out.println(gson.toJson(queryTFPair).toString());
						queryCount.put(queryTFPair.getQuery(), queryTFPair.getTermFrequency());
					}
				});
			}
			
			document.setCurrentDocumentLength(document.getTokenizedContents().size());
			documentLengthInCorpus.add(document.getCurrentDocumentLength());
			
			
			//System.out.println("*****Here in the queryCount*****");
			//System.out.println(gson.toJson(queryCount).toString());
			
			totalFrequencyInCorpus.add(queryCount);
			documentList.add(document);
			}
		
		return documentList.iterator();
	}

}
