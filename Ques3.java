package com.deepika.sparkproject;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import scala.Tuple2;
public class Ques3 implements java.io.Serializable {

	//The below variables are initialized as they would be used in program logic below
	//the logic uses calculating closePriceChange , gainSum , lossSum , incomingGainAvg , incomingLossAvg
	//finally leading into RSI calculation

	int count = 0;
	double gainSum = 0.0;
	double lossSum = 0.0;
	double RSI = 0.0;
	double RS = 0.0;
	double closePriceChange = 0.0;
	double incomingGainAvg , incomingLossAvg ;
	long prevTime = 0L ;

	//These are the objects created from RSICalculator class 
	//They keep a check on state object (which is passed from 1 window to another) , 
	//the last object (one which came in 1 minute earlier) and the new object(one which came in now)

	RSICalculator rsiCalcObj = null ;
	RSICalculator lastObj = null;
	RSICalculator newObj = null;

	public void createStreamingContext(JavaDStream<String> json) throws InterruptedException {

		//The reduce function will take a list of RSICalculator type and Optional<RSICalculator> object and 
		//return Optional<RSICalculator> object

		Function2<List<RSICalculator>, Optional<RSICalculator>, Optional<RSICalculator>> reduceFunction = new Function2<List<RSICalculator>, Optional<RSICalculator>, Optional<RSICalculator>>() {
			public Optional<RSICalculator> call(List<RSICalculator> lst, Optional<RSICalculator> obj) {

				//Assigning the state object of RSICalculator class a new object
				//When first window begins and there is no previous state, this object will be initialized 
				//to a new object of RSICalculator class (parameterless contsructor)
				//In subsequent windows, this object will be assigned the state carried from previous window
				//gainSum , lossSum , count are assigned the values derived from this object.

				rsiCalcObj = obj.or(new RSICalculator());
				gainSum=rsiCalcObj.getGainSum();
				lossSum=rsiCalcObj.getLossSum();
				count=rsiCalcObj.getCounter();

				//Running a for loop to go over the list of objects of type RSICalculator

				for(byte i=1;i<lst.size();i++) {

					//Let's create 2 objects - one for [i-1] index and one for [i] index
					lastObj= lst.get(i-1);
					newObj = lst.get(i);


					//Below variable stores the timestamp(in milliseconds) for the object at [i] index.
					prevTime= newObj.getPreviousTime();


					//set the timestamp for state object as the timestamp(this is actual timestamp , not converted to millisecods)
					//for the object at [i] index
					rsiCalcObj.setTimeStamp(newObj.getTimeStamp());


					//this is change in close price between current and last object's close price.
					closePriceChange = newObj.getClosePrice() - lastObj.getClosePrice();


					//If change in close price is negative , it is loss. If it is positive , it is gain
					//In case of loss , we take the absolute value of the loss to add to lossSum
					if (closePriceChange<0) {
						newObj.setLossValue(Math.abs(closePriceChange));
						newObj.setGainValue(0);
					}else{
						newObj.setGainValue(closePriceChange);
						newObj.setLossValue(0);
					}

					//with every passing period, we need to increase the count to keep a tab on how many periods have passed
					//Also, gainSum will be incremented by new object's closePrice gain if any
					//Similarly, lossSum will be incremented ny new object's closePrice loss if any
					//The below 'if condition' will be true till the time first RSI gets calculated
					//First RSI is calculated when count = 11, for last 10 periods. Before that, state object has 
					//avgGain , avgLoss = 0. So we are only incrementing our gain and loss sum and counter till count = 11

					if(rsiCalcObj.getAvgGain()==0 && rsiCalcObj.getAvgLoss()==0) {
						if(newObj.getPreviousTime() > rsiCalcObj.getPreviousTime()) {
							count= count+1;
							gainSum= gainSum + newObj.getGainValue();
							lossSum= lossSum + newObj.getLossValue();
						}

					}

					//This is for all subsequent RSI after 11th period
					//After the first RSI is calculated, avgGain and avgLoss will be a value , not equal to 0
					//Hence below 'if condition' will be true
					//Calculation is based on the formula shared in problem statement.

					if(rsiCalcObj.getAvgGain()!=0 && rsiCalcObj.getAvgLoss()!=0) {
						if(newObj.getPreviousTime()>rsiCalcObj.getPreviousTime()) {
							count=count+1;
							incomingGainAvg =((rsiCalcObj.getAvgGain()*9) + newObj.getGainValue())/10;
							incomingLossAvg =((rsiCalcObj.getAvgLoss()*9) + newObj.getLossValue())/10;
							rsiCalcObj.setAvgGain(incomingGainAvg);
							rsiCalcObj.setAvgLoss(incomingLossAvg);
							rsiCalcObj.setSmoothedRS(rsiCalcObj.getAvgGain() / rsiCalcObj.getAvgLoss());

							if(rsiCalcObj.getAvgLoss()==0) {
								RSI=100;
							}else if(rsiCalcObj.getAvgGain()==0) {
								RSI=0;
							}else {
								RSI=100-(100/(1+ rsiCalcObj.getSmoothedRS() ));
							}
						}
					}

					//This is the first RSI calculation, we are calculating it at count = 11 
					// for last 10 periods [10 minutes in our case]
					//We will achieve this result only during the 3rd sliding window.
					//Using the formula shared in problem statement
					//At count =11 , I also check that state object has 0 avgGain and 0 avgLoss till now

					if(count==11){
						if(rsiCalcObj.getAvgGain()==0 && rsiCalcObj.getAvgLoss()==0) {
							rsiCalcObj.setAvgGain(gainSum/10);
							rsiCalcObj.setAvgLoss(lossSum/10);
							RS = rsiCalcObj.getAvgGain() / rsiCalcObj.getAvgLoss();
							if(rsiCalcObj.getAvgLoss()==0) {
								RSI=100;
							}else if(rsiCalcObj.getAvgGain()==0) {
								RSI=0;
							}else {
								RSI=100-(100/(1+RS));
							}
						}
					}
				}

				//setting the below values in state object to be carried forward to the next window.
				rsiCalcObj.setCounter(count);
				rsiCalcObj.setRSI(RSI);
				rsiCalcObj.setGainSum(gainSum);
				rsiCalcObj.setLossSum(lossSum);
				rsiCalcObj.setPreviousTime(prevTime);

				//Call methods expects an Optional<RSICalculator> object returned
				return Optional.of(rsiCalcObj);
			}
		};

		//For this calculation I am picking up the symbol , closeprice and timestamp from the JSON 
		//bring parsed into JavaDStream.
		//For this problem specifically , I have used updateStateBykey transformation as it is stateful
		//Since RSI is not dependant just on current window but also carries the values from previous windows,
		//hence, this stateful transformation is useful;
		//window size = 10 min , sliding interval = 5 min
		//It invokes constructor from RSICalculator class 
		//Finally returns (k,V) pair of (symbol , RSI) over sliding windows

		JavaPairDStream<String, RSICalculator> pairedRDD = json.flatMap(record -> ((JsonArray) new JsonParser().parse(record)).iterator()).
				map( x -> x.getAsJsonObject().get("symbol").getAsString() + "," + x.getAsJsonObject().get("priceData").getAsJsonObject().get("close").getAsDouble() + "," + x.getAsJsonObject().get("timestamp").getAsString()).
				mapToPair(new PairFunction<String , String , RSICalculator>(){
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String , RSICalculator> call(String x){

						String[] fields = x.split(",");
						double closePrice = Double.parseDouble(fields[1]);

						//RSICalculator constructor takes 3 parameters - symbol , closePrice , timestamp of incoming stock
						return new Tuple2<String , RSICalculator>(fields[0], new RSICalculator(fields[0] , closePrice, fields[2]));
					}

				})
				.window(Durations.seconds(600), Durations.seconds(300))
				.updateStateByKey(reduceFunction);

		System.out.println("Creating the output files for Ques3 now ------");
		pairedRDD.dstream().saveAsTextFiles("Ques3_Output", "_result");
		System.out.println("RSI calculated for each stock are : ");
		pairedRDD.toJavaDStream().print();
	}
}