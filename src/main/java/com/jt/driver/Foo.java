package com.jt.driver;

import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.functions.FooFlatMap;
import com.jt.functions.FooFunction;
import com.jt.functions.FooFunction2;

/**
 * Utility class for checking or investigating scenarios in SPARK
 * 
 * @author dean
 *
 */
public class Foo {

    private static final Logger log = Logger.getLogger(Foo.class.getName());

    /**
     * Method used to generate sample data for the RDDs in the driver This can produce a simple set of records or duplicates
     * 
     * [(tupe-0,{First:0=First0}), (tupe-1,{First:1=First1}), (tupe-2,{First:2=First2}), (tupe-3,{First:3=First3})]
     * 
     * [(tupe-0,{First:0=First0}), (tupe-0,{First:0=First0}), (tupe-1,{First:1=First1}), (tupe-1,{First:1=First1}),
     * (tupe-2,{First:2=First2}), (tupe-2,{First:2=First2}), (tupe-3,{First:3=First3}), (tupe-3,{First:3=First3})]
     * 
     * @param prefix
     *            - text used to identify the key and record values in the array
     * @param elementCount
     *            - number of elements to generate in ArrayList
     * @param dup
     *            - flag to add duplicate records. Note: This will double the ArrayList size returned
     * @return ArrayList of Tuple2 to be loaded into a JavaPairRDD
     */
    public static ArrayList<Tuple2<String, GenericRecord>> buildData(String prefix, int elementCount, boolean dup) {

        Tuple2<String, GenericRecord> tup = null;

        ArrayList<Tuple2<String, GenericRecord>> vals = new ArrayList<Tuple2<String, GenericRecord>>();

        for (int a = 0; a < elementCount; a++) {
            GenericRecord gr = new GenericRecord();
            gr.put(prefix + ":" + Integer.toString(a), new String(prefix + a));

            tup = new Tuple2<String, GenericRecord>("tupe-" + a, gr);

            vals.add(tup);
            if (dup) {
                vals.add(tup);
            }
        }

        return vals;
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Foo Stuff");
        conf.setIfMissing(Constants.SPARK_MASTER, "local[4]"); // run locally if nothing is set
        JavaSparkContext context = new JavaSparkContext(conf);

        // Build up data sets using the static method provided in the Foo class
        JavaPairRDD<String, GenericRecord> leftSample1RDD = context.parallelizePairs(Foo.buildData("First", 4, true), 2);
        log.info("JPRDD 1: " + leftSample1RDD.collect());
        JavaPairRDD<String, GenericRecord> rightSample1RDD = context.parallelizePairs(Foo.buildData("Second", 6, false), 2);
        log.info("JPRDD 2: " + rightSample1RDD.collect());

        // Results of a basic join on an RDD
        JavaPairRDD<String, Tuple2<GenericRecord, GenericRecord>> joinedJPRDD = leftSample1RDD.join(rightSample1RDD);
        log.info("JoinedJPRDD: " + joinedJPRDD.collect());

        // Results of a leftOuterJoin on an RDD
        JavaPairRDD<String, Tuple2<GenericRecord, Optional<GenericRecord>>> loJPRDD = leftSample1RDD.leftOuterJoin(rightSample1RDD);
        log.info("JoinedloJPRDD: " + loJPRDD.collect());

        JavaPairRDD<String, GenericRecord> flatValJP = loJPRDD.mapValues(new FooFlatMap());
        log.info("FlatValJPRDD: " + flatValJP.collect());

        // Create a second use case for where the left side of sample are larger then the
        // right side
        JavaPairRDD<String, GenericRecord> leftSample2RDD = context.parallelizePairs(Foo.buildData("First", 6, true));
        log.info("JRDD 1: " + leftSample2RDD.collect());
        JavaPairRDD<String, GenericRecord> rightSample2RDD = context.parallelizePairs(Foo.buildData("Second", 4, false));
        log.info("JRDD 2: " + rightSample2RDD.collect());

        JavaPairRDD<String, Tuple2<GenericRecord, GenericRecord>> joinedJRDD = leftSample2RDD.join(rightSample2RDD);
        log.info("JoinedJRDD: " + joinedJRDD.collect());

        JavaPairRDD<String, Tuple2<GenericRecord, Optional<GenericRecord>>> loJRDD = leftSample2RDD.leftOuterJoin(rightSample2RDD);
        log.info("JoinedloJRDD: " + loJRDD.collect());

        JavaPairRDD<String, GenericRecord> flatVal = loJRDD.mapValues(new FooFlatMap());
        log.info("FlatValRDD: " + flatVal.collect());

        // Attempt to reduce a join
        // results were not as expected, all values in the RDD is reduced to one value
        if (false) {
            JavaPairRDD<String, Tuple2<GenericRecord, GenericRecord>> consolidated = joinedJPRDD.reduceByKey(new FooFunction2());
            log.info("Consolidated: " + consolidated.collect());

            Tuple2<String, Tuple2<GenericRecord, GenericRecord>> reduced = joinedJPRDD.reduce(new FooFunction());
            log.info("Reduced: " + reduced);

        }

    }

}
