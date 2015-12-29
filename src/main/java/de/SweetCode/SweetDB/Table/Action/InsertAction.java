package de.SweetCode.SweetDB.Table.Action;

import com.sun.deploy.util.StringUtils;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;
import de.SweetCode.SweetDB.Table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yonas on 29.12.2015.
 */
public class InsertAction {

    private Table table;
    private Syntax syntax;

    private List<Field> fields = new ArrayList<>();

    public InsertAction(Table table, Syntax syntax) {
        this.table = table;
        this.syntax = syntax;
    }

    public <T> InsertAction add(String name, T value) {
        this.fields.add(new Field<T>(name, value));
        return this;
    }

    public void build() {

        DataSet dataSet = new DataSet(this.fields);

        if(!(this.syntax.validate(dataSet))) {
            throw new IllegalArgumentException(String.format(
                    "Invalid insert query syntax, expected syntax \"%s\".",
                    this.syntax.getAsString()
            ));
        }

        this.table.insert(dataSet);

    }

}
