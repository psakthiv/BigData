package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHRankingMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterNTokenizeFlatMap;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class AssessedExercise {

	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); 
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); 
		
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[*]"; 
		
		String sparkSessionName = "BigDataAE"; 
		
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; 
		
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/SampleJson3.json"; 
		
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		spark.close();
		
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); 
		
		Encoder<NewsArticle> newsArticleEncoder = Encoders.bean(NewsArticle.class);
		Encoder<Query> queryEncoder = Encoders.bean(Query.class);
		
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), queryEncoder); 
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), newsArticleEncoder);
		
		
		Set<String> querywords = new HashSet<String>();
		
		List<Query> queryList = queries.collectAsList();
		
		queryList.forEach(query ->{
			querywords.addAll(query.getQueryTerms().stream().collect(Collectors.toSet()));
		});
		
		
		Broadcast<Set<String>> broadcastStopwords = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(querywords);
		
		Gson gson = new Gson();
		
		CollectionAccumulator<HashMap> totalFrequencyInCorpus = spark.sparkContext().collectionAccumulator();
		LongAccumulator documentLengthInCorpus = spark.sparkContext().longAccumulator();
		
		
		
		FilterNTokenizeFlatMap filterTokenizeMap = new FilterNTokenizeFlatMap(queryList, totalFrequencyInCorpus, documentLengthInCorpus);
		Encoder<Document> documentEncoder = Encoders.bean(Document.class);
		Dataset<Document> tokenizedDocument = news.flatMap(filterTokenizeMap, documentEncoder);
		
		long totalDocumentLength = tokenizedDocument.count();
		long averageDocumentLengthInCorpus = documentLengthInCorpus.value()/totalDocumentLength;
		
		
		//double averageDocumentLengthInCorpus, long totalDocsInCorpus
		DPHRankingMap dphRankingMap = new DPHRankingMap(totalFrequencyInCorpus, averageDocumentLengthInCorpus, totalDocumentLength);
		Dataset<Document> rankedDocuments = tokenizedDocument.flatMap(dphRankingMap, documentEncoder);
		rankedDocuments.count();
	

		//System.out.println(tokenizedDocument.count() + " ==> tokenizedDocument.count()");
		
		System.out.println("*******tokenizedDocumentJSON*******");
		//System.out.println(gson.toJson(tokenizedDocument.collectAsList()).toString());
		
		
		
		System.out.println("*******totalCorpus*******");
		System.out.println(gson.toJson(totalFrequencyInCorpus).toString());
		
		System.out.println("*******totalFrequencyInCorpus.value().get(0)*******");
		System.out.println(gson.toJson(totalFrequencyInCorpus.value().get(0)).toString());
		
		System.out.println("*******totalFrequencyInCorpus.value().get(0)*******");
		System.out.println(gson.toJson(totalFrequencyInCorpus.value().get(0)).toString());
		
		System.out.println("*******documentLengthInCorpus*******");
		System.out.println(gson.toJson(documentLengthInCorpus.value()).toString());
		
		
		 
		
		return null; 
	}
	
	
}
