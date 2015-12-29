package de.SweetCode.SweetDB.DataSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by Yonas on 29.12.2015.
 */
public class DataSet {

    private List<Field> fields = new ArrayList<>();

    public DataSet() {}

    public DataSet(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getFields() {
        return this.fields;
    }

    public Optional<Field> get(String name) {

        return this.fields.stream().filter(field -> field.getName().equals(name)).findFirst();

    }


}
