package de.SweetCode.SweetDB.Table.Action;

import de.SweetCode.SweetDB.Optional;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRule;
import de.SweetCode.SweetDB.Table.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yonas on 29.12.2015.
 */
public class CreateTableAction {

    private Optional<String> name = Optional.empty();
    private boolean overrideExisting = false;

    private SweetDB sweetDB;
    private List<SyntaxRule> syntax = new ArrayList<>();

    public CreateTableAction(SweetDB sweetDB) {
        this.sweetDB = sweetDB;
    }

    public CreateTableAction name(String name) {
        this.name = Optional.of(name);
        return this;
    }

    public CreateTableAction overrideExisting(boolean overrideExisting) {
        this.overrideExisting = overrideExisting;
        return this;
    }

    public CreateTableAction add(SyntaxRule syntaxRule) {
        this.syntax.add(syntaxRule);
        return this;
    }

    public boolean build() {

        if(!(this.name.isPresent())) {
            if(this.sweetDB.isDebugging()) {
                throw new IllegalArgumentException("Set the name before using the build method.");
            } else {
                return false;
            }
        }

        if(this.syntax.isEmpty()) {
            if(this.sweetDB.isDebugging()) {
                throw new IllegalArgumentException("You have to add at least one syntax rule before using the build method.");
            } else {
                return false;
            }
        }

        if(!(this.overrideExisting) && this.sweetDB.tableExist(this.name.get())) {
            return false;
        }

        if(this.overrideExisting && this.sweetDB.tableExist(this.name.get())) {
            this.sweetDB.table(this.name.get()).get().drop();
        }

        Table table = new Table(this.sweetDB, new File(this.sweetDB.getPath() + File.separator + this.name.get() + ".sweet"), this.syntax);

        this.sweetDB.addTable(table);

        if(this.sweetDB.isAutosave()) {
            table.store();
        }

        return true;

    }

}
