package com.jt.functions;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;

public class ComputeAverageTuples implements Function2<Tuple2<String, GenericRecord>, Tuple2<String, GenericRecord>, Tuple2<String, GenericRecord>> {

    @Override
    public Tuple2<String, GenericRecord> call(Tuple2<String, GenericRecord> rec1, Tuple2<String, GenericRecord> rec2) throws Exception {
        
        Double avg = ((Double)rec1._2.get(Constants.CLOSE) + (Double)rec2._2.get(Constants.CLOSE)) / 2;
        
        rec1._2.put(Constants.AVG_CLOSE, avg);
        
        return rec1;
    }

    
}
