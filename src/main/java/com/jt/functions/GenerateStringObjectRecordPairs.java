package com.jt.functions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVParser;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;

public class GenerateStringObjectRecordPairs implements PairFlatMapFunction <String, String, GenericRecord>  {
    
    private static final Logger log = Logger.getLogger(GenerateStringObjectRecordPairs.class.getName());

    private static final CSVParser csvParser = new CSVParser();
    
    private static String[] header = null;

    private  ArrayList<Tuple2<String, GenericRecord>> records = new ArrayList<Tuple2<String, GenericRecord>>();

    
    public GenerateStringObjectRecordPairs () {
        super();
    }
    
    /**
     * Constructor used to pass in the header.
     * @param header String array of the columns used in the csv data
     */
    public GenerateStringObjectRecordPairs (String[] header) {
        super();
        this.header = header;
    }
    
    @Override
    public Iterable<Tuple2<String, GenericRecord>> call(String record) throws Exception {
        records = new ArrayList<Tuple2<String, GenericRecord>>();
        String key = null;
        
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
//                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                key = values[a].substring(0,  4);
                genRec.put(header[a], values[a]);
            } else {
                genRec.put(header[a], values[a]);
            }
        }
        
        records.add(new Tuple2<String, GenericRecord>(key, genRec));
        
        return records;
    }

}
