package com.jt.functions;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.driver.StockAnalyticDriver;

/**
 * This class parses a csv record into the GenericRecord object.
 * The Spark FlatMapFunction provides a way to take an input record and transform it
 * into 0 to many objects that you will like to use through out the analytic.
 * 
 * @author dean
 *
 */
public class Transform implements Function<String, GenericRecord> {
    
    private static final Logger log = Logger.getLogger(Transform.class.getName());
    
    private static final CSVParser csvParser = new CSVParser();

    @Override
    public GenericRecord call(String record) throws Exception {
        
        if (record == null){
            return null;
        }
        
        String[] values = csvParser.parseLine(record);
        
        // check for header, if so then return nothing
        if (Arrays.equals(StockAnalyticDriver.header.value(), values)) {
            log.warn("Header and record length don't match in size");
            return null;
        }
        
        // convert the record into GenericRecord with th
        // header as a key and the column value as a value
        GenericRecord genRec = new GenericRecord();
        for (int a=0; a<values.length; a++) {
            if (StockAnalyticDriver.header.value()[a].equals(Constants.CLOSE)) {
                genRec.put(StockAnalyticDriver.header.value()[a], new Double(values[a]));
            } else {
                genRec.put(StockAnalyticDriver.header.value()[a], values[a]);
            }
        }
        
        return genRec;
        
    }
}
