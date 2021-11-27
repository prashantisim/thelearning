package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Application {
	
	public static void main(String args[]) throws InterruptedException {
		
		// Create a session
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local")
				.config("spark.executor.instances", "3")
				.config("spark.executor.cores", "1")
				.getOrCreate();
		
	   SparkContext sc = spark.sparkContext();

	   RDD<String> myRdd = sc.textFile("src/main/resources/name_and_comments.txt", 4);
	   
	   
	   
	   JavaRDD<String> jMyRdd = myRdd.toJavaRDD();
	  
	   JavaRDD<String> words = jMyRdd.flatMap((String line)-> {
		   String s[]= line.split(",");
		return  Arrays.asList(s).iterator();
	   });
	   words.toDebugString();
	   
	List<String> list =   words.collect();
	//System.out.println(list.size());
	   
	/* JavaPairRDD<String,Integer> jpr = words.mapToPair((String s)->{ 
		 
		  return new Tuple2<String,Integer>(s,1);
		  });
	 
	 
	Map<String, Integer> map = jpr.reduceByKey((v1,v2) -> v1 + v2).collectAsMap();
	 
	
	System.out.println(map);
	 
	 
	  JavaRDD<Row> theRdd = jMyRdd.map((String s)->{ 
		  String[] s2 = s.split(",");
		  return RowFactory.create(s2[0],s2[1]);
	  });
 
	
	 JavaRDD<Tuple2<String,Integer>>jpr2 = jpr.map((Tuple2<String,Integer> val) -> new Tuple2(val._1+"Ohho",val._2 ));
	 
	 
	 
	 StructField [] fields = new StructField[] {new StructField("Name",DataTypes.StringType,true,Metadata.empty()),
			 new StructField("Some",DataTypes.StringType,true,Metadata.empty())};
	 StructType schema = new StructType(fields);
	  
	  Dataset<Row> df = sqlContext.createDataFrame(theRdd, schema);
	  		Dataset<Row> df2 = spark.read().format("csv")
			.option("header", true)
		.load("src/main/resources/name_and_comments.txt");
	  
  df.repartition(2);
	  df.filter("1=1")	;
//		df.show(3);
     df.repartition(4);
     df = df.filter("Name = 'Dong'");
     df = df.withColumnRenamed("Name", "First_Name");
    
 
     
	 df.show();
	 Scanner scan = new Scanner(System.in);
	 scan.nextLine();
     df.explain();
	 
		// Transformation
	/*	df = df.withColumn("full_name", 
				concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
			.orderBy(df.col("last_name").asc());
	*/		
		// Write to destination
	/*	String dbConnectionUrl = "jdbc:postgresql://localhost/course_data"; // <<- You need to create this database
		Properties prop = new Properties();
	    prop.setProperty("driver", "org.postgresql.Driver");
	    prop.setProperty("user", "postgres");
	    prop.setProperty("password", "password"); // <- The password you used while installing Postgres
	    
	    df.write()
	    	.mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "project1", prop);
	    	*/
    Scanner scanner = new Scanner(System.in);
    scanner.nextInt();
   	spark.stop();
	
	}
}