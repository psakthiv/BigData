package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class DocumentTokenizerFlatMap implements FlatMapFunction<NewsArticle,Document>{

	@Override
	public Iterator<Document> call(NewsArticle newsArticle) throws Exception {
		Gson gson = new Gson();
		System.out.println("Inside the call in the documenttokenizerflatmap..");
		TextPreProcessor preProcessor = new TextPreProcessor();
		List<Document> documentList = new ArrayList<Document>();
		Document document = new Document();
		document.setNewsArticle(newsArticle);

		List<String> stringList = new ArrayList<String>();
		stringList.addAll(preProcessor.process(newsArticle.getTitle().toLowerCase()));
		newsArticle.getContents().forEach(content -> {
			if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
				stringList.clear();
				
				//System.out.println("*********content.getContent()**********");
				//System.out.println(content.getContent());
				stringList.addAll(preProcessor.process(content.getContent().toLowerCase()));

				//System.out.println("*********stringList**********");
				//System.out.println(stringList);
				document.setTokenizedContents(stringList);
			}
			
		});
		
		Set<String> set = new HashSet<String>();
		set = document.getTokenizedContents().stream().collect(Collectors.toSet());
		
		document.setTokenizedContents(new ArrayList<>());
		document.setTokenizedContents(set.stream().collect(Collectors.toList()));
		document.setCurrentDocumentLength(document.getTokenizedContents().size());
		documentList.add(document);
		return documentList.iterator();
	}

}
