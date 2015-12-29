package de.SweetCode.SweetDB.DataType;

/**
 * Created by Yonas on 29.12.2015.
 */
public interface DataType {

    String getName();

    String getSyntax();

    <T> T parse(String value);

}
