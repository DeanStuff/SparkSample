package com.jt.functions;

import java.util.logging.Logger;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.jt.commons.GenericRecord;

public class FooFlatMap implements Function<Tuple2<GenericRecord, Optional<GenericRecord>>, GenericRecord> {

    private static final Logger log = Logger.getLogger(FooFlatMap.class.getName());

    @Override
    public GenericRecord call(Tuple2<GenericRecord, Optional<GenericRecord>> v1) throws Exception {

        if (v1._2 == null) {
            log.warning("found v1 as null");
            return v1._1();
        }
        
        
        GenericRecord gr1 = new GenericRecord();
        for (String key : v1._1().keySet()) {
            gr1.put(key, v1._1.get(key));
        }
        
        if (v1._2().isPresent()) {
            for (String key : v1._2().get().keySet()) {
                gr1.put(key, v1._2.get().get(key));
            }
        }
        
        return gr1;
    }

}
