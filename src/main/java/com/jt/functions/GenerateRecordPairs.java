package com.jt.functions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;
import com.jt.driver.SpikeDetection;

/**
 * This class is using the FlatMapFunction to replace the Filter and Function classes.
 * Or simply doing two things in one class.
 * 
 * @author dean
 *
 */

public class GenerateRecordPairs implements PairFlatMapFunction <String, Date, GenericRecord> {
    
    private static final Logger log = Logger.getLogger(GenerateRecordPairs.class.getName());

    private static final CSVParser csvParser = new CSVParser();
    
    private static String[] header = null;

    private  ArrayList<Tuple2<Date, GenericRecord>> records = new ArrayList<Tuple2<Date, GenericRecord>>();

    
    public GenerateRecordPairs () {
        super();
    }
    
    public GenerateRecordPairs (String[] header) {
        super();
        this.header = header;
    }
    
    @Override
    public Iterable<Tuple2<Date, GenericRecord>> call(String record) throws Exception {
        
        records = new ArrayList<Tuple2<Date, GenericRecord>>();
        Date key = null;
        
        if (record == null){
            return records;
        }
        
        String[] values = csvParser.parseLine(record);
        
        // check for header, if so then return nothing
        if (Arrays.equals(header, values)  || (header.length != values.length)) {
            return records;
        }
        
        
        // convert the record into GenericRecord with the
        // header as a key and the column value as a value
        GenericRecord genRec = new GenericRecord();
        for (int a=0; a<values.length; a++) {
            if (header[a].equals(Constants.CLOSE) ||
                (header[a].equals(Constants.HIGH)) ||
                (header[a].equals(Constants.LOW))) {
                genRec.put(header[a], new Double(values[a]));
            } else if (header[a].equals(Constants.DATE)) {
                // to be thread safe, we have to create a new simpledateformat and
                // date object each time
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                key = sdf.parse(values[a]);
                genRec.put(header[a], key);
            } else {
                genRec.put(header[a], values[a]);
            }
        }
        
        records.add(new Tuple2<Date, GenericRecord>(key, genRec));
        
        return records;
        
    }

}
