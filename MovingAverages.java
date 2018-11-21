package com.deepika.sparkproject;

public class MovingAverages implements java.io.Serializable{

	private static final long serialVersionUID = 1L;
	
	//the member variables for MovingAverages class.
	//these variables are used for calculations of moving averages for closing price
	//and for profit calculations (average close price - average open price)
	
	String symbol;
	int count;
	double closePrice ; 
	double openPrice;
	double closeSum;
	double openSum;
	double closeAverage;
	double openAverage;
	double profit;
	
	//this contructor is called from Moving Average for closing price class 
	public MovingAverages(double close, String symbol) {
		this.symbol=symbol;
		this.closePrice = close;
	}
	
	//this constructor is called from Profit calculation class
	public MovingAverages(double close, double open) {
		this.closePrice = close;
		this.openPrice = open;
	}
	
	//the getter and setter methods for all member variables
	public double getClosePrice() {
		return closePrice;
	}
	public void setClosePrice(double closePrice) {
		this.closePrice = closePrice;
	}
	
	public double getOpenPrice() {
		return openPrice;
	}
	public void setOpenPrice(double openPrice) {
		this.openPrice = openPrice;
	}
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public double getCloseSum() {
		return closeSum;
	}
	public void setCloseSum(double closeSum) {
		this.closeSum = closeSum;
	}
	public double getCloseAverage() {
		return closeAverage;
	}
	public void setCloseAverage(double closeAverage) {
		this.closeAverage = closeAverage;
	}
	public double getOpenSum() {
		return openSum;
	}
	public void setOpenSum(double openSum) {
		this.openSum = openSum;
	}
	public double getOpenAverage() {
		return openAverage;
	}
	public void setOpenAverage(double openAverage) {
		this.openAverage = openAverage;
	}
	public double getProfit() {
		return this.profit;
	}
	public void setProfit(double profit) {
		this.profit = profit;
	}
}