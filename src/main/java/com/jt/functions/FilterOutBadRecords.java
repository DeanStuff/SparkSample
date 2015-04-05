package com.jt.functions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Utils;
import com.jt.driver.StockAnalyticDriver;

/**
 * This class flags out records that don't comply with the number of fields
 * expected in the csv file.
 * This can also be used to validate field values as well.
 * 
 * @author dean
 *
 */
public class FilterOutBadRecords implements Function<String, Boolean> {
    
    private static final Logger log = Logger.getLogger(FilterOutBadRecords.class.getName());

    private static final CSVParser csvParser = new CSVParser();

    @Override
    public Boolean call(String record){
        
        String[] values;
        try {
            values = csvParser.parseLine(record);
        } catch (IOException e) {
            log.warn("Couldn't parse record: " + record + "  message: " + e.getMessage());
            return false;  // bad data, can't parse
        }
        
        if (values.length != StockAnalyticDriver.header.value().length) {
            return false;
        }
        
        try {
            Utils.simpleDateFormat.parse(values[0]);
        } catch (ParseException e) {
            log.warn("Couldn't parse date from record: " + values[0] + "  message: " + e.getMessage());
            return false;  // bad date format
        }
        
        return true;
    }
    

}
