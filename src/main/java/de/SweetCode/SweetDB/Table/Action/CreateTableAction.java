package de.SweetCode.SweetDB.Table.Action;

import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;

/**
 * Created by Yonas on 29.12.2015.
 */
public class CreateTableAction {

    private SweetDB sweetDB;
    private Syntax syntax = new Syntax();

    public CreateTableAction(SweetDB sweetDB) {
        this.sweetDB = sweetDB;
    }

    public CreateTableAction addSyntax(String fieldName, DataType dataType) {
        this.syntax.add(fieldName, dataType);
        return this;
    }

    public void build() {



    }

}
