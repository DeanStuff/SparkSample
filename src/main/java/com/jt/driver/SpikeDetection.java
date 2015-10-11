package com.jt.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
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

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.functions.ComputeAverageTuples;
import com.jt.functions.ComputeAverages;
import com.jt.functions.GenerateRecordPairs;
import com.jt.functions.GenerateStringObjectRecordPairs;
import com.jt.functions.LargestOneDaySwingReduce;
import com.jt.functions.LargestOneDaySwingReduce2;
import com.jt.functions.NullRecordFilter;

/**
 * This class shall demonstrate one approach in analyzing data to reflect s
 * @author dean
 *
 */
public class SpikeDetection {
    
    private static final Logger log = Logger.getLogger(SpikeDetection.class.getName());
    
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
    
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Finding Spikes");
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
        
        // copy out to executioners like distributed cache
        header = context.broadcast(SpikeDetection.getHeader(inputFilename));

        // Ok, up to this point the analytic is very much like the one that
        // computes averages.  This time I will skip the filter stage and use
        // a function which will return zero or more values.  
        // Essentially, handling 2 things in one stage since the functionality is
        // the same the performance can be improved this way.  This process removes
        // the CSV record from being parsed in two stages and brings it to one.
        // Oh, and later examples will use the mapr components within Spark to
        // utilize the framework to its be ability.  Right now, lets do it long
        // hand to get a feel of the framework.
        
        JavaPairRDD<Date, GenericRecord> recordsKeyedByYMD = inputRDD.flatMapToPair(new GenerateRecordPairs(header.value()));
        if (recordsKeyedByYMD != null) {
            recordsKeyedByYMD.values().saveAsTextFile(outputPath+"/records");
        }
        System.out.println("Number of records: " + recordsKeyedByYMD.count());
        
        // Build record objects and key by year
        JavaPairRDD<String, GenericRecord> recordKeyedByY = inputRDD.flatMapToPair(new GenerateStringObjectRecordPairs(header.value()));
        
        
        JavaPairRDD<Date, GenericRecord> filteredRecords = recordsKeyedByYMD.filter(new NullRecordFilter());
        filteredRecords.saveAsTextFile(outputPath+"/filtered");
        System.out.println("Number of results after flter: " + filteredRecords.count());
        
        JavaPairRDD<Date, GenericRecord> largestHighLowOneDay = recordsKeyedByYMD.sortByKey();
        largestHighLowOneDay.saveAsTextFile(outputPath+"/recordsOrderedByDate");
        largestHighLowOneDay = largestHighLowOneDay.reduceByKey(new LargestOneDaySwingReduce());
        System.out.println("Number of results: " + largestHighLowOneDay.count());
        largestHighLowOneDay.saveAsTextFile(outputPath+"/largestOneDay");
        
        // Example of using an action.  Remember, the reduce method is an action as opposed to a transformation
        Tuple2<Date, GenericRecord> largestHighLowOneDay2 = recordsKeyedByYMD.sortByKey().reduce(new LargestOneDaySwingReduce2());
        System.out.println("Highest low high found: " + largestHighLowOneDay2._1.toString() + " and value is: " + largestHighLowOneDay2._2.toString());
        
        JavaPairRDD<Date, GenericRecord> orderedByDate = recordsKeyedByYMD.sortByKey();
        
        JavaPairRDD<String, GenericRecord> largestHighLowInYear = recordKeyedByY.reduceByKey(new LargestOneDaySwingReduce());
        largestHighLowInYear.sortByKey().saveAsTextFile(outputPath+"/largestByYeary");
        
        largestHighLowInYear = recordKeyedByY.reduceByKey(new ComputeAverages());
        largestHighLowInYear.sortByKey().saveAsTextFile(outputPath+"/avgByYeary");
    }
    
    

}
