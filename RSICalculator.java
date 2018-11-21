package com.deepika.sparkproject;
import java.text.SimpleDateFormat;

public class RSICalculator implements java.io.Serializable{

	private static final long serialVersionUID = 1L;
	private String symbol = null;
	private String timeStamp = null;
	private double closePrice = 0;
	private double gainSum = 0;
	private double lossSum = 0;
	private double gainValue = 0;
	private double lossValue = 0;
	private int counter = 0;
	private double avgGain = 0;
	private double avgLoss = 0;
	private double RSI = 0;
	private double smoothedRS = 0;
	
	private long previousTime = 0L;
	
	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public double getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(double closePrice) {
		this.closePrice = closePrice;
	}

	public double getGainSum() {
		return gainSum;
	}

	public void setGainSum(double gainSum) {
		this.gainSum = gainSum;
	}

	public double getLossSum() {
		return lossSum;
	}

	public void setLossSum(double lossSum) {
		this.lossSum = lossSum;
	}

	public double getGainValue() {
		return gainValue;
	}

	public void setGainValue(double gainValue) {
		this.gainValue = gainValue;
	}

	public double getLossValue() {
		return lossValue;
	}

	public void setLossValue(double lossValue) {
		this.lossValue = lossValue;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	public double getAvgGain() {
		return avgGain;
	}

	public void setAvgGain(double avgGain) {
		this.avgGain = avgGain;
	}

	public double getAvgLoss() {
		return avgLoss;
	}

	public void setAvgLoss(double avgLoss) {
		this.avgLoss = avgLoss;
	}

	public double getRSI() {
		return RSI;
	}

	public void setRSI(double rSI) {
		RSI = rSI;
	}

	public long getPreviousTime() {
		return previousTime;
	}

	public void setPreviousTime(long previousTime) {
		this.previousTime = previousTime;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public double getSmoothedRS() {
		return smoothedRS;
	}

	public void setSmoothedRS(double smoothedRS) {
		this.smoothedRS = smoothedRS;
	}

	
	//constructor with no arguments
	RSICalculator(){

	}
	
	//constructor with parameters - symbol , closePrice and time at which you recieved it
	RSICalculator(String symbol , double closePrice, String prevTimestamp){
		this.symbol = symbol;
		this.closePrice=closePrice;
		timeStamp=prevTimestamp;
		previousTime=getMilliSeconds(timeStamp);

	}

	
	//Below method takes the incoming timestamp , parses it and converts it 
	//into a format "yyyy-MM-dd HH:mm:ss" and extracts equivalent milliseconds from the time
	
    public static long getMilliSeconds(String datetime) {
    	long timeMillis = 0L;
        if (datetime ==null || datetime.trim().length()==0) {
            return 0;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
        	timeMillis = dateFormat.parse(datetime).getTime();
        } catch (Exception e) {
            e.getMessage();
        }
        return timeMillis ;
    }
    
    //This method has been implemented to specifically be picked up by print() method  
    //to print RSI value only and not whole RSI object
    
    public String toString() {
		return String.valueOf(getRSI());
	}
}
