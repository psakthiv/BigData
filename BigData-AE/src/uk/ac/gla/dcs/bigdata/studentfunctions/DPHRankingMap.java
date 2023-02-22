package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTFPair;

public class DPHRankingMap implements FlatMapFunction<Document, Document>{
	

	
	/**
	 * Calculates the DPH score for a single query term in a document
	 * @param termFrequencyInCurrentDocument // The number of times the query appears in the document
	 * @param totalTermFrequencyInCorpus // the number of times the query appears in all documents
	 * @param currentDocumentLength // the length of the current document (number of terms in the document)
	 * @param averageDocumentLengthInCorpus // the average length across all documents
	 * @param totalDocsInCorpus // the number of documents in the corpus
	 * @return
	 */
	
	CollectionAccumulator<HashMap> totalFrequencyInCorpus;
	double averageDocumentLengthInCorpus;
	long totalDocsInCorpus;
	
	public DPHRankingMap (CollectionAccumulator<HashMap> totalFrequencyInCorpus, double averageDocumentLengthInCorpus, long totalDocsInCorpus) {
		this.totalFrequencyInCorpus = totalFrequencyInCorpus;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
	}
	
	@Override
	public Iterator<Document> call(Document document) throws Exception {
		Gson gson = new Gson();
		System.out.println("Here inside the DPHRankingMap.....");
		short termFrequencyInCurrentDocument  = 0;
		HashMap<Query, Integer> totalFrequency = totalFrequencyInCorpus.value().get(0);
		for(QueryTFPair queryPair: document.getNoOfOccurances()) {
			termFrequencyInCurrentDocument += 	queryPair.getTermFrequency();
			
			if(termFrequencyInCurrentDocument != 0) {
				System.out.println("*******totalFrequencyInCorpus.value().get(0) in DPHRankingMap*******");
				System.out.println(gson.toJson(totalFrequencyInCorpus.value().get(0)).toString());
				int corpusFrequency = (int)totalFrequency.get(queryPair.getQuery());
				System.out.println("*****corpusFrequency*****");
				System.out.println(corpusFrequency);
				double dphScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, corpusFrequency, document.getCurrentDocumentLength(), 
						averageDocumentLengthInCorpus, totalDocsInCorpus);
				System.out.println("*****dphScore*****");
				System.out.println(dphScore);
				document.setScore(dphScore);
			}	
			
		}
		
		
		return null;
	}

}
