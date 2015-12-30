package de.SweetCode.SweetDB.DataSet;

import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Table;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Field<T> {

    private SweetDB sweetDB;
    private Table table;

    private String name;
    private T value;

    public Field(SweetDB sweetDB, Table table, String name, T value) {
        this.sweetDB = sweetDB;
        this.table = table;
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the name of the field.
     * @return
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the value of the field.
     * @return
     */
    public T getValue() {
        return this.value;
    }

    /**
     *
     * @param dataType
     * @param <V>
     * @return
     */
    public <V> V as(DataType dataType) {
        return dataType.parse(this.value.toString());
    }

    /**
     *
     * @param clazz
     * @param <V>
     * @return
     */
    public <V> V as(Class<V> clazz) {
        return clazz.cast(this.value);
    }

    /**
     * Updates the value.
     * @param value
     */
    public void update(T value) {
        this.value = value;

        if(this.sweetDB.isAutosave()) {
            this.table.store();
        }
    }

}
