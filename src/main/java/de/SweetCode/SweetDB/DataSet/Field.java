package de.SweetCode.SweetDB.DataSet;

import de.SweetCode.SweetDB.DataType.DataType;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Field<T> {

    private String name;
    private T value;

    public Field(String name, T value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return this.name;
    }

    public T getValue() {
        return this.value;
    }

    public <V> V as(DataType dataType) {
        return dataType.parse(this.value.toString());
    }

    public <V> V as(Class<V> clazz) {
        return clazz.cast(this.value);
    }

    public void update(T value) {
        this.value = value;
    }

}
