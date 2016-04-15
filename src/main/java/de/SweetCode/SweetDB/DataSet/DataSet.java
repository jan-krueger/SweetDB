package de.SweetCode.SweetDB.DataSet;

import de.SweetCode.SweetDB.Optional;
import de.SweetCode.SweetDB.Table.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Yonas on 29.12.2015.
 */
public class DataSet {

    private Table table;
    private List<Field> fields = new ArrayList<>();

    public DataSet(Table table) {
        this.table = table;
    }

    public DataSet(Table table, List<Field> fields) {
        this(table);
        this.fields = fields;
    }

    public List<Field> getFields() {
        return this.fields;
    }

    public void addField(Field field) {
        this.fields.add(field);
    }

    public Optional<Field> get(String name) {

        for(Field field : this.fields) {
            if(field.getName().equals(name)) {
                return Optional.of(field);
            }
        }

        return Optional.empty();
    }

    public boolean delete() {

        Iterator<DataSet> iterator = this.table.all().iterator();

        if(!(iterator.hasNext())) {
            return false;
        }

        while(iterator.hasNext()) {

            if(iterator.next().equals(this)) {
                iterator.remove();
                break;
            }

        }

        if(this.table.getDatabase().isAutosave()) {
            this.table.store();
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {

        if(o == null) {
            return false;
        }

        if(!(o instanceof DataSet)) {
            return false;
        }

        if(o == this) {
            return true;
        }

        DataSet dataSet = (DataSet) o;

        for(Field field : dataSet.getFields()) {

            if(!(this.get(field.getName()).isPresent())) {
                return false;
            }

            if(!(field.getValue().equals(this.get(field.getName()).get().getValue()))) {
                return false;
            }

        }

        return true;
    }

}
