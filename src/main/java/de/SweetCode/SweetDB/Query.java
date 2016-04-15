package de.SweetCode.SweetDB;

import de.SweetCode.SweetDB.DataSet.DataSet;

/**
 * Created by Yonas on 15.04.2016.
 */
public interface Query {

    boolean matches(DataSet dataSet);

}
