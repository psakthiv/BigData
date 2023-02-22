package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class Document {
	
	private NewsArticle newsArticle;
	private List<String> tokenizedContents;
	private List<QueryTFPair> noOfOccurances;
	private int currentDocumentLength;
	private double score;
	private double totalTFInCorpus;
	
	public NewsArticle getNewsArticle() {
		return newsArticle;
	}
	public void setNewsArticle(NewsArticle newsArticle) {
		this.newsArticle = newsArticle;
	}
	
	public List<String> getTokenizedContents() {
		return tokenizedContents;
	}
	public void setTokenizedContents(List<String> tokenizedContents) {
		this.tokenizedContents = tokenizedContents;
	}
	
	public List<QueryTFPair> getNoOfOccurances() {
		return noOfOccurances;
	}
	public void setNoOfOccurances(List<QueryTFPair> noOfOccurances) {
		this.noOfOccurances = noOfOccurances;
	}
	
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}
	
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	public double getTotalTFInCorpus() {
		return totalTFInCorpus;
	}
	public void setTotalTFInCorpus(double totalTFInCorpus) {
		this.totalTFInCorpus = totalTFInCorpus;
	}
	

}
