package com.dpq.country.data.driver;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class CountryDataAnalysis {

	static Map<String,BigDecimal> referenceData =new HashMap<>();
	static {
		referenceData.put("US",new BigDecimal("1"));
		referenceData.put("AU",new BigDecimal("1.25"));
		referenceData.put("KR",new BigDecimal("1.75"));
		
	}
	public static void main(String[] args) throws InterruptedException {
		 JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
	        
	   // JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("RDD_EXAMPLES").setMaster("local")); 
	    Broadcast<Map<String, BigDecimal>> broadcastedMap = sc.broadcast(referenceData);
	    //Country,Reporting_purpuse,Quarter,Grain,pp0,pp1
	    JavaRDD<String> fileRdd = sc.textFile("/Users/dpq/springbootWrokspace/CountryDataAnalysis/resources/modeloutput.csv",4);
		JavaRDD<ModelOutput> modelOutputRdd = fileRdd.map(data -> new ModelOutput(data.split(",")[0] ,data.split(",")[1] ,data.split(",")[2] 
				,data.split(",")[3] ,new BigDecimal(data.split(",")[4]),new BigDecimal(data.split(",")[5]) ) );
		//System.out.println("\n\n\n\n\n\n\n\n\nmodelOutputRdd:"+modelOutputRdd.take(1));
		
		JavaRDD<ModelOutput> modelOutputRddWithFxRate = modelOutputRdd.map(data -> getModelOutputWithFxRate(data,broadcastedMap));
		//System.out.println("\n\n\n\n\n\n\n\n\nmodelOutputRdd:"+modelOutputRddWithFxRate.take(1));
		
		JavaRDD<ModelOutput> modelOutputFilteredRDD = modelOutputRddWithFxRate.filter(data -> !data.getQuarter().contains("2019"));
		//System.out.println("\n\n\n\n\n\n\n\n\nmodelOutputRdd:"+modelOutputFilteredRDD.count());
		
		JavaPairRDD<ModelOutputKey, ModelOutput> modeloutputPairRDD = modelOutputFilteredRDD.flatMapToPair(new PairFlatMapFunction<ModelOutput, ModelOutputKey, ModelOutput>(){
			List<Tuple2<ModelOutputKey, ModelOutput>> tuplePairs = new LinkedList<>();
			@Override
			public Iterator<Tuple2<ModelOutputKey, ModelOutput>> call(ModelOutput input) throws Exception {
				tuplePairs.add(new Tuple2<>(new ModelOutputKey(input.getCountry() , input.getQuarter() , input.getGrain()) , input));
				return tuplePairs.iterator();
			}
		});
		
		//System.out.println("\n\n\n\n\n\n\nmodeloutputPairRDD:   "+modeloutputPairRDD.collect());
		
		JavaPairRDD<ModelOutputKey, ModelOutput> modeloutputreducebyRDD = modeloutputPairRDD.reduceByKey(new Function2<ModelOutput, ModelOutput, ModelOutput>() {
			
			@Override
			public ModelOutput call(ModelOutput arg0, ModelOutput arg1) throws Exception {
				arg0.setPp0(arg0.getPp0().add(arg1.getPp0()));
				arg0.setPp1(arg0.getPp1().add(arg1.getPp1()));
				return arg0;
			}
		});
		
		System.out.println("modeloutputreducebyRDD######################     :    "+modeloutputreducebyRDD.collect());
		
		
		sc.close();
	}
	
	
	
	public static ModelOutput getModelOutputWithFxRate(ModelOutput modelOutputRdd,Broadcast<Map<String, BigDecimal>>  referenceData) {
		modelOutputRdd.setPp0(modelOutputRdd.getPp0().multiply(referenceData.getValue().get(modelOutputRdd.getCountry())));
		modelOutputRdd.setPp1(modelOutputRdd.getPp1().multiply(referenceData.getValue().get(modelOutputRdd.getCountry())));
		return modelOutputRdd;
	}
}

class ModelOutputKey implements Serializable{
	private String country;
	private String quarter;
	private String grain;
	
	public ModelOutputKey(String country, String quarter, String grain) {
		super();
		this.country = country;
		this.quarter = quarter;
		this.grain = grain;
	}
	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getQuarter() {
		return quarter;
	}
	public void setQuarter(String quarter) {
		this.quarter = quarter;
	}
	public String getGrain() {
		return grain;
	}
	public void setGrain(String grain) {
		this.grain = grain;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		result = prime * result + ((grain == null) ? 0 : grain.hashCode());
		result = prime * result + ((quarter == null) ? 0 : quarter.hashCode());
		return result;
	}
	
	
	
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ModelOutputKey other = (ModelOutputKey) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (grain == null) {
			if (other.grain != null)
				return false;
		} else if (!grain.equals(other.grain))
			return false;
		if (quarter == null) {
			if (other.quarter != null)
				return false;
		} else if (!quarter.equals(other.quarter))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ModelOutput [country=" + country + ", quarter=" + quarter + ", grain=" + grain + "]";
	}
}

class ModelOutput implements Serializable{
	private String country;
	private String reportingPurpose;
	private String quarter;
	private String grain;
	private BigDecimal pp0;
	private BigDecimal pp1;
	
	public ModelOutput(String country, String reportingPurpose, String quarter, String grain, BigDecimal pp0,
			BigDecimal pp1) {
		super();
		this.country = country;
		this.reportingPurpose = reportingPurpose;
		this.quarter = quarter;
		this.grain = grain;
		this.pp0 = pp0;
		this.pp1 = pp1;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	
	public String getReportingPurpose() {
		return reportingPurpose;
	}
	public void setReportingPurpose(String reportingPurpose) {
		this.reportingPurpose = reportingPurpose;
	}
	public String getQuarter() {
		return quarter;
	}
	public void setQuarter(String quarter) {
		this.quarter = quarter;
	}
	public String getGrain() {
		return grain;
	}
	public void setGrain(String grain) {
		this.grain = grain;
	}
	public BigDecimal getPp0() {
		return pp0;
	}
	public void setPp0(BigDecimal pp0) {
		this.pp0 = pp0;
	}
	public BigDecimal getPp1() {
		return pp1;
	}
	public void setPp1(BigDecimal pp1) {
		this.pp1 = pp1;
	}
	@Override
	public String toString() {
		return "ModelOutput [country=" + country + ", reportingPurpose=" + reportingPurpose + ", quarter=" + quarter
				+ ", grain=" + grain + ", pp0=" + pp0 + ", pp1=" + pp1 + "]";
	}
	
}
