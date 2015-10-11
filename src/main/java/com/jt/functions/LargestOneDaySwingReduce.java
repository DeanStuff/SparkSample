package com.jt.functions;

import org.apache.spark.api.java.function.Function2;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;

public class LargestOneDaySwingReduce implements Function2 <GenericRecord,  GenericRecord, GenericRecord> {

    private double threshold = 0.0;
    
    private double value = 0.0;
    
    
    public LargestOneDaySwingReduce() {
        super();
    }

    public LargestOneDaySwingReduce(double threshold) {
        super();
        this.threshold = threshold;
    }

    @Override
    public GenericRecord call(GenericRecord v1, GenericRecord v2) throws Exception {
        if (v1 == null) {
            return v2;
        }
        
        if (v2 == null) {
            return v1;
        }
        
        if ( ( (Double)v1.get(Constants.HIGH) - (Double)v1.get(Constants.LOW)) >= ((Double)v2.get(Constants.HIGH) - (Double)v2.get(Constants.LOW)) ) {
            return v1;
        }
        
        return v2;
    }

}
