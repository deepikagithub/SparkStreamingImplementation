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

public class Ques4 implements java.io.Serializable {

	public void createStreamingContext(JavaDStream<String> json) throws InterruptedException {

		//This is the summary function which adds the volume in incoming stream to current stream's volume

		Function2<Integer , Integer , Integer> reduceFunc = new Function2<Integer , Integer , Integer>(){

			@Override public Integer call(Integer valuePresent , Integer valueIncoming) throws Exception{

				//System.out.println("Summary function is running");
				//System.out.println(valuePresent + "+" + valueIncoming);
				return valuePresent + valueIncoming ;
			}
		};


		//This is the inverse function which subtracts the volume of outgoing stream from current stream's volume
		Function2<Integer , Integer , Integer> invReduceFunc = new Function2<Integer , Integer , Integer>() {

			@Override public Integer call(Integer valuePresent , Integer valueOutgoing) throws Exception{

				//System.out.println("inverse function is runing");
				//System.out.println(valuePresent + "-" + valueOutgoing);
				return valuePresent - valueOutgoing ;
			}
		};


		//For this calculation I am picking up the symbol and closeprice from the JSON 
		//bring parsed into JavaDStream.
		//reduceByKeyAndWindow takes 4 arguments - summary func , inverse func, 
		//window size = 10 min , sliding interval = 10 min
		//Finally returns (k,V) pair of (symbol , volume) over sliding windows

		JavaPairDStream<String, Integer> pairedRDD= json.flatMap(record -> ((JsonArray) new JsonParser().parse(record)).iterator()).
				map( x -> x.getAsJsonObject().get("symbol").getAsString() + "," + x.getAsJsonObject().get("priceData").getAsJsonObject().get("volume").getAsInt()).
				mapToPair(new PairFunction<String , String , Integer>(){
					@Override
					public Tuple2<String , Integer> call(String row){

						String[] fields = row.split(",");
						String symbol = fields[0];
						int volume = Integer.parseInt(fields[1]);
						return new Tuple2<String , Integer>(symbol , volume);
					}

				}).reduceByKeyAndWindow(reduceFunc, invReduceFunc, Durations.seconds(600) , Durations.seconds(600)).
				mapToPair(
						x -> new Tuple2<String , Integer>(x._1 , x._2)
						);

		//Swap the (k , v) pair as we need to sort in descending order based on value and not key
		JavaPairDStream<Integer , String> swappedPair = pairedRDD.mapToPair(x -> x.swap());

		//Sort the (k , v) pairs now based on key and sort in descending order
		JavaPairDStream<Integer,String> sortedStream = swappedPair.transformToPair(
				new Function<JavaPairRDD<Integer,String>, JavaPairRDD<Integer,String>>() {

					public JavaPairRDD<Integer,String> call(JavaPairRDD<Integer,String> jPairRDD) throws Exception {
						return jPairRDD.sortByKey(false);
					}
				});


		//This will output the results generated at 10 min sliding intervals
		//The output will be written to text files
		//The files will get created in workspace in folders named as below:

		System.out.println("Creating the output files for Ques4 now ------");
		sortedStream.dstream().saveAsTextFiles("Ques4_Output", "_result");
		System.out.println("Volume for all stocks in descending order are : ");
		sortedStream.print();
	} 

}
