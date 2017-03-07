package com.demo.common;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 描述 ：日期工具类
 *
 * @author : mzb
 * @version : v1.00
 * @CreationDate : 16-6-12 下午13:50
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class DateUtil {

    public final static String DATE_FORMATER_MONTH = "yyyy-MM";
    public final static String DATE_FORMATER = "yyyy-MM-dd";
    public final static String DATE_SECOND_FORMATER = "yyyy-MM-dd HH:mm:ss";
    public final static String DATE_FORMATER_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public final static String DATE_NOYEAR = "MM/dd HH:mm:ss";


    /**
     * "yyyy-MM-dd"转Date
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static Date convertToDate(String dateStr) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATER);
        return sdf.parse(dateStr);
    }

    /**
     * "yyyy-MM-dd HH:mm:ss"转Date
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static Date convertToDateTime(String dateStr) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_SECOND_FORMATER);
        return sdf.parse(dateStr);
    }

    /**
     * Date转"yyyy-MM-dd"
     * @param date
     * @return
     */
    public static String convertToDateString(Date date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATER);
        return sdf.format(date);
    }

    /**
     * Date转"yyyy-MM"
     * @param date
     * @return
     * @throws ParseException
     */
    public static String convertToDateMonthString(Date date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATER_MONTH);
        return sdf.format(date);
    }

    /**
     * Date转"yyyy-MM-dd HH:mm:ss"
     * @param date
     * @return
     */
    public static String convertToDateTimeString(Date date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_SECOND_FORMATER);
        return sdf.format(date);
    }

    /**
     * 日期加天数
     * @param date
     * @param num
     * @return
     */
    public static Date addDateDays(Date date,int num){
        Calendar cd = Calendar.getInstance();
        cd.setTime(date);
        cd.add(Calendar.DATE, num);//增加一天
        return cd.getTime();
    }

    /**
     * 日期加月数
     * @param date
     * @param num
     * @return
     */
    public static Date addDateMonths(Date date,int num){
        Calendar cd = Calendar.getInstance();
        cd.setTime(date);
        cd.add(Calendar.MONTH, num);//增加一月
        return cd.getTime();
    }

    /**
     * 日期加年数
     * @param date
     * @param num
     * @return
     */
    public static Date addDateYears(Date date,int num){
        Calendar cd = Calendar.getInstance();
        cd.setTime(date);
        cd.add(Calendar.YEAR, num);//增加一月
        return cd.getTime();
    }

    /**
     * 获取系统当前时间
     * @return
     */
    public static Date getSystemDate(){
        return new Date();
    }

    public static String convertToBDateTimeString(Date date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATER_YYYYMMDDHHMMSS);
        return sdf.format(date);
    }

    /**
     * 将短时间格式转换为时间 yyyy-MM-dd
     *
     * @param date
     * @return
     */
    public static Date dateToDate(Date date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATER);
        return sdf.parse(sdf.format(date));
    }

    /**
     * Date转"MM/dd HH:mm:ss"
     * @param date
     * @return
     */
    public static String convertToDateNoYear(Date date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_NOYEAR);
        return sdf.format(date);
    }
}
