package com.jt.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.functions.ComputeAverages;
import com.jt.functions.FilterOutBadRecords;
import com.jt.functions.GenericKV;
import com.jt.functions.Transform;

/**
 * This is a basic example of a Spark analytic to process a csv file and to perform stuff.
 * Why stuff, so I can cover the basics of how tos and approaces in the API.
 * 
 * Oh, and this is a simplistic breakdown of building a Spark analytic so I can potentially
 * make and reuse the function implementation within the Spark API.  Note, I only did this
 * because the inline and monstrous examples that I have seen get a bit challenging to follow
 * after a few hunderd lines in the main method.
 * 
 * @author dean
 *
 */
public class StockAnalyticDriver {
    
    private static final Logger log =  Logger.getLogger(StockAnalyticDriver.class.getName());
    
    public static Broadcast<String[]> header = null; 
    
    /**
     * Yield to the safe side and use the HDFS so that both local and hadoop file
     * systems could be used in this example.
     * @param filename
     * @throws IOException 
     */
    public static String[] getHeader(String filename) throws IOException {
        Configuration hadoopConfig = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        
        Path path = new Path(filename);
        FSDataInputStream inputStream = fileSystem.open(path);
        
        // read out the header line, only 1 line
        BufferedReader bufferedReader  = new BufferedReader(new InputStreamReader(inputStream));
        String firstLine = bufferedReader.readLine();
        
        CSVParser parser = new CSVParser();
        String[] header = parser.parseLine(firstLine);
        inputStream.close();
        
        return header;
        
    }
    
    public static void main(String args[]) throws Exception {

        SparkConf conf = new SparkConf().setAppName("StockAnalytic");
        conf.setIfMissing(Constants.SPARK_MASTER, "local"); // run locally if nothing is set
        JavaSparkContext context = new JavaSparkContext(conf);
        
        String inputFilename = null;
        String outputPath = null;
        
        if (args.length > 0) {
            inputFilename = args[0];
            outputPath = args[1];
        } else if (System.getProperty(Constants.INPUT_PATH) != null) {
            inputFilename = System.getProperty(Constants.INPUT_PATH);
            outputPath = System.getProperty(Constants.OUTPUT_PATH);
        } else {
            throw new Exception("Must supply an input either as a parameter or property, don't forget to use hdfs:/// or file:///");
        }
        
        JavaRDD<String> inputRDD = context.textFile(inputFilename);
        
        // In this example I know the first row of the csv file has the header.
        // I will show how to use the broadcast class to share the header to all
        // the executioners
        
        // copy out to executioners like distributed cache
        header = context.broadcast(StockAnalyticDriver.getHeader(inputFilename));
        
        // filter out records
        JavaRDD<String> goodRDD = inputRDD.filter(new FilterOutBadRecords());
        
        // convert strings to the application objects
        JavaRDD<GenericRecord> convertedRDD = goodRDD.map(new Transform());
        
        // use the mapper and make key/value pairs for the reducer
        JavaPairRDD<String, GenericRecord> yearlyRDD = convertedRDD.mapToPair(new GenericKV());
        
        // computer the average in a function.  But I think it is possible to layout
        // the values into an RDD of doubles and take the mean of it.  But this example
        // shows how something can be done in the reduce phase.
        JavaPairRDD<String, GenericRecord> yearlyAverages = yearlyRDD.reduceByKey(new ComputeAverages());
        
        // write the part files out
        yearlyAverages.saveAsTextFile(outputPath);
    }

}
