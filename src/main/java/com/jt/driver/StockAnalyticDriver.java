package com.jt.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.functions.ComputeAverage;
import com.jt.functions.ComputeAverages;
import com.jt.functions.FilterOutBadRecords;
import com.jt.functions.GenericKV;
import com.jt.functions.Transform;

/**
 * This is a basic example of a Spark analytic to process a csv file and to perform stuff. Why stuff, so I can cover the basics of
 * how tos and approaces in the API.
 * 
 * Oh, and this is a simplistic breakdown of building a Spark analytic so I can potentially make and reuse the function
 * implementation within the Spark API. Note, I only did this because the inline and monstrous examples that I have seen get a bit
 * challenging to follow after a few hunderd lines in the main method.
 * 
 * @author dean
 *
 */
public class StockAnalyticDriver {

    private static final Logger log = Logger.getLogger(StockAnalyticDriver.class.getName());

    private static String topic = null;

    public static Broadcast<String[]> header = null;

    /**
     * Yield to the safe side and use the HDFS so that both local and hadoop file systems could be used in this example.
     * 
     * @param filename
     * @throws IOException
     */
    public static String[] getHeader(String filename) throws IOException {
        Configuration hadoopConfig = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConfig);

        Path path = new Path(filename);
        FSDataInputStream inputStream = fileSystem.open(path);

        // read out the header line, only 1 line
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String firstLine = bufferedReader.readLine();

        CSVParser parser = new CSVParser();
        String[] header = parser.parseLine(firstLine);
        inputStream.close();

        return header;

    }

    public static void main(String args[]) throws Exception {

        SparkConf conf = new SparkConf().setAppName("StockAnalytic");
        conf.setIfMissing(Constants.SPARK_MASTER, "local[4]"); // run locally if nothing is set
        JavaSparkContext context = new JavaSparkContext(conf);

        String inputFilename = null;
        String outputPath = null;

        if (args.length > 0) {
            inputFilename = args[0];
            outputPath = args[1];

            if (args.length > 2) {
                topic = args[2];
            }

        } else if (System.getProperty(Constants.INPUT_PATH) != null) {
            inputFilename = System.getProperty(Constants.INPUT_PATH);
            outputPath = System.getProperty(Constants.OUTPUT_PATH);
        } else {
            throw new Exception("Must supply an input either as a parameter or property, don't forget to use hdfs:/// or file:///");
        }

        // setup and get RDD for data in a stream
        if (topic != null) {
            // Create the context with a 1 second batch size
            JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
            HashMap<String, Integer> topicThreadMap = new HashMap<String, Integer>();
            topicThreadMap.put(topic, 1);
            
            JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "localhost:2181", "StockAnalytic", topicThreadMap);

        }

        // setup and get RDD for data in a text file

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

        // computer the average in a function. But I think it is possible to layout
        // the values into an RDD of doubles and take the mean of it. But this example
        // shows how something can be done in the reduce phase.
        JavaPairRDD<String, GenericRecord> yearlyAverages = yearlyRDD.reduceByKey(new ComputeAverage());

        // write the part files out
        yearlyAverages.saveAsTextFile(outputPath);

        // ok, so lets mess with the ability to calculate the mean without the reducer. But instead
        // use the more efficient internals of spark.
        // JavaPairRDD<String, Double> yearlyCloseRDD = yearlyRDD.reduceByKey(new ComputeAverages());
    }

}
