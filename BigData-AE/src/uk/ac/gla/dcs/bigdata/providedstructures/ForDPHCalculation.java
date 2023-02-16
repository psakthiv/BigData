package uk.ac.gla.dcs.bigdata.providedstructures;

public class ForDPHCalculation {
	
	private String documentID;
	private String queryTerm;
	private int termFrequencyInCurrentDocument;
	private int totalTermFrequencyInCorpus;
	private int currentDocumentLength;
	private int averageDocumentLengthInCorpus;
	private int totalDocsInCorpus;
	
	public String getQueryTerm() {
		return queryTerm;
	}
	public void setQueryTerm(String queryTerm) {
		this.queryTerm = queryTerm;
	}
	
	public String getDocumentID() {
		return documentID;
	}
	public void setDocumentID(String documentID) {
		this.documentID = documentID;
	}
	
	public int getNoOfOccurances() {
		return termFrequencyInCurrentDocument;
	}
	public void setNoOfOccurances(int noOfOccurances) {
		this.termFrequencyInCurrentDocument = noOfOccurances;
	}
	
	public int getLength() {
		return totalTermFrequencyInCorpus;
	}
	public void setLength(int length) {
		this.totalTermFrequencyInCorpus = length;
	}
	
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}
	
	public int getAverageDocumentLengthInCorpus() {
		return averageDocumentLengthInCorpus;
	}
	public void setAverageDocumentLengthInCorpus(int averageDocumentLengthInCorpus) {
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
	}
	
	public int getTotalDocsInCorpus() {
		return totalDocsInCorpus;
	}
	public void setTotalDocsInCorpus(int totalDocsInCorpus) {
		this.totalDocsInCorpus = totalDocsInCorpus;
	}
	
	

}
