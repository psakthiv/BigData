package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
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
	CollectionAccumulator<DocumentRanking> documentRankingList;
	
	public DPHRankingMap (CollectionAccumulator<HashMap> totalFrequencyInCorpus, double averageDocumentLengthInCorpus, long totalDocsInCorpus, 
			CollectionAccumulator<DocumentRanking> documentRankingList) {
		this.totalFrequencyInCorpus = totalFrequencyInCorpus;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
		this.documentRankingList = documentRankingList;
	}
	
	@Override
	public Iterator<Document> call(Document document) throws Exception {
		List<Document> docList = new ArrayList<Document>();
		Gson gson = new Gson();
		//System.out.println("Here inside the DPHRankingMap.....");
		short termFrequencyInCurrentDocument  = 0;
		HashMap<Query, Integer> totalFrequency = totalFrequencyInCorpus.value().get(0);
		for(QueryTFPair queryPair: document.getNoOfOccurances()) {
			termFrequencyInCurrentDocument += 	queryPair.getTermFrequency();
			
			if(termFrequencyInCurrentDocument != 0) {
				//System.out.println("*******totalFrequencyInCorpus.value().get(0) in DPHRankingMap*******");
				//System.out.println(gson.toJson(totalFrequencyInCorpus.value().get(0)).toString());
				int corpusFrequency = (int)totalFrequency.get(queryPair.getQuery());
				//System.out.println("*****corpusFrequency*****");
				/*System.out.println(corpusFrequency + ", document.getCurrentDocumentLength() " + document.getCurrentDocumentLength()
				+ ", averageDocumentLengthInCorpus" + averageDocumentLengthInCorpus + ", totalDocsInCorpus " + totalDocsInCorpus);*/
				double dphScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, corpusFrequency, document.getCurrentDocumentLength(), 
						averageDocumentLengthInCorpus, totalDocsInCorpus);
//				System.out.println("*****dphScore*****");
	//			System.out.println(dphScore);
				document.setScore(dphScore);
			}	
			
		}
		
		
		List<DocumentRanking> documentRankings = documentRankingList.value();
		
		documentRankings.forEach(documentRanking -> {
			document.getNoOfOccurances().forEach(noOfOccurance -> {
				if(noOfOccurance.getQuery().equals(documentRanking.getQuery())){
					RankedResult rankedResult = new RankedResult();
					rankedResult.setArticle(document.getNewsArticle());
					rankedResult.setScore(document.getScore());
					rankedResult.setDocid(document.getNewsArticle().getId());
					documentRanking.getResults().add(rankedResult);
					documentRankingList.add(documentRanking);
				}
			});
			
		});
		if(Objects.isNull(documentRankings) || documentRankings.size() == 0) {
			document.getNoOfOccurances().forEach(noOfOccurance -> {
					DocumentRanking documentRanking = new DocumentRanking();
					documentRanking.setQuery(noOfOccurance.getQuery());
					RankedResult rankedResult = new RankedResult();
					rankedResult.setArticle(document.getNewsArticle());
					rankedResult.setScore(document.getScore());
					rankedResult.setDocid(document.getNewsArticle().getId());
					List<RankedResult> rankedResults = new ArrayList<RankedResult>();
					rankedResults.add(rankedResult);
					documentRanking.setResults(rankedResults);
					documentRankingList.add(documentRanking);
			});
		}
		docList.add(document);
		
		return docList.iterator();
	}

}
