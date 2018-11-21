package com.deepika.sparkproject;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;




import scala.Tuple2;
public class Ques1 implements java.io.Serializable {


	public void createStreamingContext(JavaDStream<String> json) throws InterruptedException {

		//This is the summary function to calculate closing average that includes incoming RDDs in the sliding window
		//This adds the new closeprice to existing close price , increments the count and calculates new 
		//closing price average

		Function2<MovingAverages, MovingAverages, MovingAverages> reduceFunc = new Function2<MovingAverages, MovingAverages, MovingAverages>() {
			@Override 
			public MovingAverages call(MovingAverages valueExisting, MovingAverages valueIncoming) throws Exception {

				valueExisting.setCloseSum(valueExisting.getCloseSum() + valueIncoming.getClosePrice());
				valueExisting.setCount(valueExisting.getCount() + 1);
				valueExisting.setCloseAverage(valueExisting.getCloseSum() / valueExisting.getCount());
				return valueExisting;
			}
		};


		//This is the inverse function to calculate closing average that excludes outgoing RDDs in the sliding window
		//This subtracts the outgoing closeprice from existing close price , decrements the count and calculates new 
		//closing price average

		Function2<MovingAverages, MovingAverages, MovingAverages> invReduceFunc = new Function2<MovingAverages, MovingAverages, MovingAverages>() {
			@Override 
			public MovingAverages call(MovingAverages valueExisting, MovingAverages valueOutgoing) throws Exception {

				valueExisting.setCloseSum(valueExisting.getCloseSum() - valueOutgoing.getClosePrice());
				valueExisting.setCount(valueExisting.getCount() - 1);
				valueExisting.setCloseAverage(valueExisting.getCloseSum() / valueExisting.getCount());
				return valueExisting;
			}
		};

		//For this calculation I am picking up the symbol and closeprice from the JSON 
		//bring parsed into JavaDStream.
		//reduceByKeyAndWindow takes 4 arguments - summary func , inverse func, 
		//window size = 10 min , sliding interval = 5 min
		//It invokes constructor from MovingAverages class 
		//Finally returns (k,V) pair of (symbol , closingaverage) over sliding windows

		JavaPairDStream<String, Double> pairedRDD = json.flatMap(record -> ((JsonArray) new JsonParser().parse(record)).iterator()).
				map( x -> x.getAsJsonObject().get("symbol").getAsString() + "," + x.getAsJsonObject().get("priceData").getAsJsonObject().get("close").getAsDouble()).
				mapToPair(new PairFunction<String , String , MovingAverages>(){
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String , MovingAverages> call(String row){

						String[] fields = row.split(",");
						double closePrice = Double.parseDouble(fields[1]);
						return new Tuple2<String , MovingAverages>(fields[0], new MovingAverages(closePrice , fields[0]));
					}

				}).reduceByKeyAndWindow(reduceFunc,invReduceFunc, Durations.seconds(600), Durations.seconds(300)).
				mapToPair(
						x -> new Tuple2<String , Double>(x._1 , x._2.getCloseAverage())
						);


		//This will output the results generated at 5 min intervals over 10 min sliding window
		//The output will be written to text files
		//The files will get created in workspace in folders named as below:

		System.out.println("Creating the output files for Ques1 now ------");
		pairedRDD.dstream().saveAsTextFiles("Ques1_Output", "_result");
		System.out.println("Moving average closing prices for 4 stocks : ");
		pairedRDD.dstream().print();
	} 
}
