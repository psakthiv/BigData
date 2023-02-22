package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTFPair;

public class DocumentRankingFlatMap implements FlatMapFunction<Document,DocumentRanking>{
	
	private List<Query> queries;
	private	LongAccumulator noOfMatchInCorpus;
	
	//termFrequencyInCurrentDocument // The number of times the query appears in the document
	//totalTermFrequencyInCorpus // the number of times the query appears in all documents
	//currentDocumentLength // the length of the current document (number of terms in the document)
	//averageDocumentLengthInCorpus // the average length across all documents
	//totalDocsInCorpus // the number of documents in the corpus
	
	public DocumentRankingFlatMap (LongAccumulator noOfMatchInCorpus, List<Query> queries) {
		this.queries = queries;
		this.noOfMatchInCorpus = noOfMatchInCorpus;
	}

	@Override
	public Iterator<DocumentRanking> call(Document document) throws Exception {
		List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();
		
		System.out.println("Here comes the DocumentRankingFlatMap..");
		Gson gson = new Gson();
		System.out.println("*******queryJSON*******");
		System.out.println(gson.toJson(queries).toString());

		
		System.out.println("*******documentJSON*******");
		System.out.println(gson.toJson(document).toString());

		
		System.out.println("*******DocumentRankingFlatMap*******");

		List<String> tokens = document.getTokenizedContents();
		
		
		queries.stream().forEach(query ->{
			List<String> queryTerms = query.getQueryTerms();
			queryTerms.stream().forEach(queryTerm -> {
				
				tokens.stream().forEach(token -> {
					//System.out.println(gson.toJson(token).toString());
					if(token.toLowerCase().toString().equals(queryTerm.toLowerCase().toString())) {
						System.out.println("Found facebook..");
						System.out.println(token.toLowerCase().toString() + " ==> token.toLowerCase().toString()");
						System.out.println(queryTerm.toLowerCase().toString() + " ==> queryTerm.toLowerCase().toString()");
						
					
						System.out.println("At the end of the loop..");
					}
				});
				
			});
		});
		
		
		
		

		return documentRankingList.iterator();
	}

}
