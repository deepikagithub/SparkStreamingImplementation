package com.deepika.sparkproject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StockAnalysis implements java.io.Serializable{

	//This is the driver class for the entire application

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		StockAnalysis sparkMainApplication = new StockAnalysis();

		try {
			sparkMainApplication.createStreamingContext(args[0]);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	//Below method takes an input , which is the folder path (location of that folder) at which Python is generating 
	//JSONs. 

	private void createStreamingContext(String inputDir) throws InterruptedException {

		//Setting the spark context - spark conf object and batch interval for streaming data

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkapplication");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(60));
		streamingContext.checkpoint("checkpoint_dir");
		Logger.getRootLogger().setLevel(Level.ERROR);



		//JSON created by python script are being saved at location specified below
		//Reading the JSON and parsing it to create DStreams.

		JavaDStream<String> json = streamingContext.textFileStream(inputDir);

		//Creating objects for each class as below

		Ques1 ques1 = new Ques1();
		Ques2 ques2 = new Ques2();
		Ques3 ques3 = new Ques3();
		Ques4 ques4 = new Ques4();

		//Calling the createStreamingContext method from type one by one

		ques1.createStreamingContext(json);
		ques2.createStreamingContext(json);
		ques3.createStreamingContext(json);
		ques4.createStreamingContext(json);

		//This is closing the streamingContext
		streamingContext.start();
		streamingContext.awaitTermination();
		streamingContext.close();


	}

}