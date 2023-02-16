package test.main;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class TestMain {

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
		
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); 
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); 
		
		List<NewsArticle> newsArticles = news.collectAsList();
		List<NewsArticle> finalArticles = new ArrayList<NewsArticle>();
		
		newsArticles.forEach(newsArticle -> { 
			System.out.println(newsArticle.getAuthor());
			List<ContentItem> finalContents = new ArrayList<ContentItem>();
			
			newsArticle.getContents().forEach(content -> {
				int count = 0;
				System.out.println("Here inside the first forEach statement..");
				System.out.println(content);
				if(!StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase())){
					System.out.println("Here inside the first if statement..");
					count++;
					if(finalContents.size() < 5) {
						
						System.out.println("Here inside the second if statement..");
						finalContents.add(content);
						newsArticle.setContents(finalContents);
						finalArticles.add(newsArticle);
					}
					System.out.println(count + " ==> Count..");
				}
			});
			
		});
		
		
		return null; 
	}
	
}
