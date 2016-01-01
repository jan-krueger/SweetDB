package de.SweetCode.SweetDB.DataSet;

import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.Table.Table;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Field<T> {

    private Table table;

    private String name;
    private T value;

    public Field(Table table, String name, T value) {
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
    public boolean update(T value) {

        if(this.value == value) {
            return true;
        }

        T tmp = this.value;
        this.value = value;

        if(!(this.table.getSyntax().validate(this))) {

            this.value = tmp;

            if(this.table.getDatabase().isDebugging()) {
                throw new IllegalArgumentException("You provided an invalid value.");
            } else {
                return false;
            }

        }

        if(this.table.getDatabase().isAutosave()) {
            this.table.store();
        }

        return true;

    }

}
