package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class Document {
	
	private String id; 
	private NewsArticle newsArticle;
	private List<String> tokenizedContents;
	private List<NameValuePair> noOfOccurances;
	private int currentDocumentLength;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
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
	
	public List<NameValuePair> getNoOfOccurances() {
		return noOfOccurances;
	}
	public void setNoOfOccurances(List<NameValuePair> noOfOccurances) {
		this.noOfOccurances = noOfOccurances;
	}
	
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}
	
	public void updateTokenizedContents(List<String> tokenizedContents) {
		if(Objects.isNull(this.tokenizedContents)){
			this.tokenizedContents = new ArrayList<String>();
		}
		this.tokenizedContents.addAll(tokenizedContents);
		
	}
	

}
