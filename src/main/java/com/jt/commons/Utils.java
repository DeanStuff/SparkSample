package com.jt.commons;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    
    public static Date parseDate(String val) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.parse(val);
    }

    
}
