package com.jt.functions;

import java.util.Date;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;

public class LargestOneDaySwingReduce2 implements Function2 <Tuple2<Date,GenericRecord>,  Tuple2<Date,GenericRecord>, Tuple2<Date,GenericRecord>> {

    private double threshold = 0.0;
    
    private double value = 0.0;
    
    
    public LargestOneDaySwingReduce2() {
        super();
    }

    public LargestOneDaySwingReduce2(double threshold) {
        super();
        this.threshold = threshold;
    }

    @Override
    public Tuple2<Date,GenericRecord> call(Tuple2<Date,GenericRecord> v1, Tuple2<Date,GenericRecord> v2) throws Exception {
        if (v1 == null) {
            return v2;
        }
        
        if (v2 == null) {
            return v1;
        }
        
        if ( ( (Double)v1._2.get(Constants.HIGH) - (Double)v1._2.get(Constants.LOW)) >= ((Double)v2._2.get(Constants.HIGH) - (Double)v2._2.get(Constants.LOW)) ) {
            return v1;
        }
        
        return v2;
    }

}
