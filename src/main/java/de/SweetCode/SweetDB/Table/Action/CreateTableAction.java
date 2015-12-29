package de.SweetCode.SweetDB.Table.Action;

import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;
import de.SweetCode.SweetDB.Table.Table;

import java.io.File;
import java.util.Optional;

/**
 * Created by Yonas on 29.12.2015.
 */
public class CreateTableAction {

    private Optional<String> name = Optional.empty();

    private SweetDB sweetDB;
    private Syntax syntax = new Syntax();

    public CreateTableAction(SweetDB sweetDB) {
        this.sweetDB = sweetDB;
    }

    public CreateTableAction name(String name) {
        this.name = Optional.of(name);
        return this;
    }

    public CreateTableAction add(String fieldName, DataType dataType) {
        this.syntax.add(fieldName, dataType);
        return this;
    }

    public void build() {

        if(!(this.name.isPresent())) {
            throw new IllegalArgumentException("Set the name before using the build method.");
        }

        if(this.syntax.getSyntax().isEmpty()) {
            throw new IllegalArgumentException("You have to add at least one syntax rule before using the build method.");
        }

        Table table = new Table(this.sweetDB, new File(this.sweetDB.getPath() + File.separator + this.name.get() + ".sweet"), this.syntax);

        this.sweetDB.addTable(table);

        if(this.sweetDB.isAutosave()) {
            table.store();
        }

    }

}
