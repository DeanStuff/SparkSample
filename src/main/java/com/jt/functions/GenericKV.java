package com.jt.functions;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.jt.commons.GenericRecord;
import com.jt.commons.Utils;
import com.jt.driver.StockAnalyticDriver;

/**
 * Generic way of creating a key/value pair.  Keeping the key a basic String object
 * and passing the GenericRecord down the path for further processing other thoughts
 * that might come to mind.
 * 
 * 
 * @author dean
 *
 */
public class GenericKV implements PairFunction <GenericRecord, String, GenericRecord> {
    
    private static final Logger log = Logger.getLogger(GenericKV.class.getName());

    @Override
    public Tuple2<String, GenericRecord> call(GenericRecord record) throws Exception  {
        
        Date date;
        try {
            date = Utils.parseDate((String)record.get(StockAnalyticDriver.header.getValue()[0]));
        } catch (ParseException e) {
            log.warn("Couldn't parse date format: " + record.get(StockAnalyticDriver.header.getValue()[0]), e);
            throw e;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        return  new Tuple2<String, GenericRecord>(String.valueOf(cal.get(Calendar.YEAR)), record);
    }

}
