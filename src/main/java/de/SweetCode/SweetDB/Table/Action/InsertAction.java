package de.SweetCode.SweetDB.Table.Action;

import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;
import de.SweetCode.SweetDB.Table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yonas on 29.12.2015.
 */
public class InsertAction {

    private SweetDB sweetDB;
    private Table table;
    private Syntax syntax;

    private List<Field> fields = new ArrayList<>();

    public InsertAction(SweetDB sweetDB, Table table, Syntax syntax) {
        this.sweetDB = sweetDB;
        this.table = table;
        this.syntax = syntax;
    }

    /**
     * Adds the value of a field to the current insertion action.
     * @param name
     * @param value
     * @param <T>
     * @return
     */
    public <T> InsertAction add(String name, T value) {
        this.fields.add(new Field(this.table, name, value));
        return this;
    }


    public boolean build() {

        DataSet dataSet = new DataSet(this.table, this.fields);

        if(!(this.syntax.validate(dataSet))) {
            if(this.sweetDB.isDebugging()) {
                throw new IllegalArgumentException(String.format(
                        "Invalid insert query.",
                        this.syntax.getAsString()
                ));
            } else {
                return false;
            }
        }

        this.table.insert(dataSet);
        return true;

    }

}
