package com.jt.functions;

import java.util.logging.Logger;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.jt.commons.GenericRecord;

public class FooFunction implements
       Function2<Tuple2<String, Tuple2<GenericRecord, GenericRecord>>, Tuple2<String, Tuple2<GenericRecord, GenericRecord>>, Tuple2<String, Tuple2<GenericRecord, GenericRecord>>> {

    private static final Logger log = Logger.getLogger(FooFunction.class.getName());

    @Override
    public Tuple2<String, Tuple2<GenericRecord, GenericRecord>> call(Tuple2<String, Tuple2<GenericRecord, GenericRecord>> v1, Tuple2<String, Tuple2<GenericRecord, GenericRecord>> v2) throws Exception {

        if (v1 == null) {
            log.warning("found v1 as null");
            return v2;
        }
        
        if (v2 == null) {
            log.warning("found v2 as null");
            return v1;
        }
        
        if (v1._1() == null && v1._2() == null) {
            log.warning("found v1 and v2 as null");
           return v2;
        }
        
        if (v2._1() == null && v2._2() == null) {
            log.warning("found v1 and v2 as null");
            return v1;
         }
                
        GenericRecord gr1 = new GenericRecord();
        gr1.putAll(v1._2()._1());
        gr1.putAll(v1._2()._2());
        
        GenericRecord gr2 = new GenericRecord();
        gr2.putAll(v2._2()._1());
        gr2.putAll(v2._2()._2());
        
        
        return new Tuple2(new String(v1._1() + "-" + v2._1()), new Tuple2(gr1, gr2));
    }
}
