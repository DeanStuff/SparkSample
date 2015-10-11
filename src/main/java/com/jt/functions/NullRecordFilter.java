package com.jt.functions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.GenericRecord;
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
public class NullRecordFilter implements Function<Tuple2<Date, GenericRecord>, Boolean> {
    
    private static final Logger log = Logger.getLogger(NullRecordFilter.class.getName());

    private static final CSVParser csvParser = new CSVParser();
    
    private static String[] header = null;
    
    

    public NullRecordFilter() {
        super();
    }

    public NullRecordFilter(String[] header) {
        super();
        this.header = header;
    }

    @Override
    public Boolean call(Tuple2<Date, GenericRecord> record){
        
        return  record._1 != null && !record._2.isEmpty();
    }
    

}
