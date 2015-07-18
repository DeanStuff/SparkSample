package com.jt.functions;

import org.apache.spark.api.java.function.Function2;

import com.jt.commons.Constants;
import com.jt.commons.GenericRecord;

public class ComputeAverages implements Function2<GenericRecord, GenericRecord, Double> {

    @Override
    public Double call(GenericRecord rec1, GenericRecord rec2) throws Exception {
        
        Double avg = ((Double)rec1.get(Constants.CLOSE) + (Double)rec2.get(Constants.CLOSE)) / 2;
        
        return avg;
    }

    
}
