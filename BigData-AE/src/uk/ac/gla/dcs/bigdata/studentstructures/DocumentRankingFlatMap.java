package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

public class DocumentRankingFlatMap implements FlatMapFunction<Document,DocumentRanking>{
	
	private Broadcast<Set<String>> broadcastQuerywords;
	private	LongAccumulator noOfMatchInCorpus;
	
	
	public DocumentRankingFlatMap (LongAccumulator noOfMatchInCorpus, Broadcast<Set<String>> broadcastQuerywords) {
		this.broadcastQuerywords = broadcastQuerywords;
		this.noOfMatchInCorpus = noOfMatchInCorpus;
	}

	@Override
	public Iterator<DocumentRanking> call(Document document) throws Exception {
		List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();
		
		System.out.println("Here comes the DocumentRankingFlatMap..");
		Gson gson = new Gson();

		System.out.println("*******DocumentRakingFlatMap*******");
		System.out.println(gson.toJson(broadcastQuerywords.value()));
		
		broadcastQuerywords.value().forEach(broadcastQueryWord -> {
			
			document.getTokenizedContents().forEach(token -> {
				if(token.equals(broadcastQueryWord)) {
					if(Objects.nonNull(document.getNoOfOccurances())) {
						document.getNoOfOccurances().forEach(occurance -> {
							if(occurance.getName().equals(broadcastQueryWord)){
								int currentValue = Integer.parseInt(occurance.getValue());
								int nextValue = currentValue++;
								occurance.setValue(String.valueOf(nextValue));
							}
							else {
								occurance.setValue(String.valueOf(1));
							}
							
						});
					}
					else {
						NameValuePair nvp = new NameValuePair();
						nvp.setName(broadcastQueryWord);
						nvp.setValue(String.valueOf(1));
						List<NameValuePair> nvpList = new ArrayList<NameValuePair>();
						nvpList.add(nvp);
						document.setNoOfOccurances(nvpList);
					}
				}
			});
		});
		
		

		return documentRankingList.iterator();
	}

}
