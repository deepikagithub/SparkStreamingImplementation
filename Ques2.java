package com.deepika.sparkproject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class Ques2 implements java.io.Serializable {

	public void createStreamingContext(JavaDStream<String> json) throws InterruptedException {


		//This is the summary function to calculate closing average that includes incoming RDDs in the sliding window
		//This adds the new closeprice to existing close price avg, increments the count and calculates new 
		//closing price average
		//Similarly, this adds the new openprice to existing open price avg, increments the count and calculates new 
		//open price average. Finally profit is calculated as -> avg.closingprice - avg.openprice

		Function2<MovingAverages, MovingAverages, MovingAverages> reduceFunc = new Function2<MovingAverages, MovingAverages, MovingAverages>() {
			@Override 
			public MovingAverages call(MovingAverages valueExisting, MovingAverages valueIncoming) throws Exception {

				valueExisting.setCount(valueExisting.getCount() + 1);
				valueExisting.setCloseSum(valueExisting.getCloseSum() + valueIncoming.getClosePrice());
				valueExisting.setOpenSum(valueExisting.getOpenSum() + valueIncoming.getOpenPrice());
				valueExisting.setCloseAverage(valueExisting.getCloseSum() / valueExisting.getCount());
				valueExisting.setOpenAverage(valueExisting.getOpenSum() / valueExisting.getCount());
				valueExisting.setProfit(valueExisting.getCloseAverage() - valueExisting.getOpenAverage());
				return valueExisting;
			}
		};

		//This is the inverse function to calculate closing average that excludes outgoing RDDs in the sliding window
		//This subtracts the outgoing closeprice from existing close price avg, decrements the count and calculates new 
		//closing price average
		//Similarly, this subtracts the outgoing openprice from existing open price avg , decrements the count and calculates new 
		//open price average. Finally profit is calculated as -> avg.closingprice - avg.openprice

		Function2<MovingAverages, MovingAverages, MovingAverages> invReduceFunc = new Function2<MovingAverages, MovingAverages, MovingAverages>() {
			@Override 
			public MovingAverages call(MovingAverages valueExisting, MovingAverages valueOutgoing) throws Exception {
				valueExisting.setCount(valueExisting.getCount() - 1);
				valueExisting.setCloseSum(valueExisting.getCloseSum() - valueOutgoing.getClosePrice());
				valueExisting.setOpenSum(valueExisting.getOpenSum() - valueOutgoing.getOpenPrice());
				valueExisting.setCloseAverage(valueExisting.getCloseSum() / valueExisting.getCount());
				valueExisting.setOpenAverage(valueExisting.getOpenSum() / valueExisting.getCount());
				valueExisting.setProfit(valueExisting.getCloseAverage() - valueExisting.getOpenAverage());
				return valueExisting;
			}
		}; 


		//For this calculation I am picking up the symbol , openprice and closeprice from the JSON 
		//bring parsed into JavaDStream.
		//reduceByKeyAndWindow takes 4 arguments - summary func , inverse func, 
		//window size = 10 min , sliding interval = 5 min
		//It invokes constructor from MovingAverages class which takes openPrice and closeprice as parameter
		//Finally returns (k,V) pair of (symbol , profit) over sliding windows

		JavaPairDStream<String, Double> pairedRDD = json.flatMap(record -> ((JsonArray) new JsonParser().parse(record)).iterator()).
				map( x -> x.getAsJsonObject().get("symbol").getAsString() + "," + x.getAsJsonObject().get("priceData").getAsJsonObject().get("close").getAsDouble()
						+ "," + x.getAsJsonObject().get("priceData").getAsJsonObject().get("open").getAsDouble()).
				mapToPair(new PairFunction<String , String , MovingAverages>(){


					@Override
					public Tuple2<String , MovingAverages> call(String row){

						String[] fields = row.split(",");
						double closePrice = Double.parseDouble(fields[1]);
						double openPrice = Double.parseDouble(fields[2]);
						return new Tuple2<String , MovingAverages>(fields[0], new MovingAverages(closePrice,openPrice));
					}

				}).reduceByKeyAndWindow(reduceFunc,invReduceFunc, Durations.seconds(600), Durations.seconds(300)).
				mapToPair(
						x -> new Tuple2<String , Double>(x._1 , x._2.getProfit())
						);


		//Swap the (k , v) pair as we need to sort in descending order based on value and not key
		JavaPairDStream<Double , String> swappedPair = pairedRDD.mapToPair(x -> x.swap());

		//Sort the (k , v) pairs now based on key and sort in descending order
		JavaPairDStream<Double,String> sortedStream = swappedPair.transformToPair(
				new Function<JavaPairRDD<Double,String>, JavaPairRDD<Double,String>>() {

					public JavaPairRDD<Double,String> call(JavaPairRDD<Double,String> jPairRDD) throws Exception {
						return jPairRDD.sortByKey(false);
					}
				});


		//This will output the results generated at 5 min intervals over 10 min sliding window
		//The output will be written to text files
		//The files will get created in workspace in folders named as below:

		System.out.println("Creating the output files for Ques2 now ------");
		sortedStream.dstream().saveAsTextFiles("Ques2_Output", "_result");
		System.out.println("Profits of 4 stocks in descending order : ");
		sortedStream.dstream().print();

	} 

}

