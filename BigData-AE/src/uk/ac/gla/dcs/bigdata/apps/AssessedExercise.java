package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
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
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;

import uk.ac.gla.dcs.bigdata.functions.flatmap.ContentFilterFlatMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentTokenizerFlatMap;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentRankingFlatMap;

public class AssessedExercise {

	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); 
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); 
		
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; 
		
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
		if (newsFile==null) newsFile = "data/SampleJson.json"; 
		
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
		LongAccumulator noOfMatchInCorpus = spark.sparkContext().longAccumulator();
		
		
		ContentFilterFlatMap newsArticleMap = new ContentFilterFlatMap();
		Dataset<NewsArticle> finalNewsArticles = news.flatMap(newsArticleMap, newsArticleEncoder);
		System.out.println(finalNewsArticles.count());
		
		DocumentTokenizerFlatMap documentTokenizer = new DocumentTokenizerFlatMap();
		Encoder<Document> documentEncoder = Encoders.bean(Document.class);
		Dataset<Document> document = news.flatMap(documentTokenizer, documentEncoder);
		System.out.println(document.count());
		
		DocumentRankingFlatMap documentRakingFlatMap = new DocumentRankingFlatMap(noOfMatchInCorpus, broadcastStopwords);
		Encoder<DocumentRanking> documentRankingEncoder = Encoders.bean(DocumentRanking.class);
		Dataset<DocumentRanking> documentRanking = document.flatMap(documentRakingFlatMap, documentRankingEncoder);
		System.out.println(documentRanking.count());
		
		
		Gson gson = new Gson();

		System.out.println("*******articleJSON*******");
		System.out.println(gson.toJson(finalNewsArticles.collectAsList()).toString());
		
		System.out.println("*******queryJSON*******");
		System.out.println(gson.toJson(queries.collectAsList()).toString());
		
		
		//System.out.println("*******documentJSON*******");
		//System.out.println(gson.toJson(document.collectAsList()).toString());
		 
		
		return null; 
	}
	
	
}
