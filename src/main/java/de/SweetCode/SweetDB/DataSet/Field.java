package de.SweetCode.SweetDB.DataSet;

import com.sun.istack.internal.logging.Logger;
import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Table;

import java.util.logging.Level;

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
        return (V) dataType.parse(this.value.toString());
    }

    /**
     *
     * @param clazz
     * @param <V>
     * @return
     */
    public <V> V as(V clazz) {
        return (V) clazz.getClass().cast(this.value);
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
                Logger.getLogger(SweetDB.class).log(
                        Level.INFO,
                        String.format(
                                "SweetDB - Field (Method: Update) (%s|%s) - Invalid Value: %s",
                                this.getName(),
                                this.value.toString(),
                                value.toString()
                        )
                );
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
