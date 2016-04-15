package de.SweetCode.SweetDB.Table.Syntax;


import com.google.gson.JsonObject;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.Query;
import de.SweetCode.SweetDB.Table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Syntax {

    private Table table;

    private HashMap<String, SyntaxRule> syntax = new HashMap<>();

    public Syntax(Table table) {
        this.table = table;
    }

    /**
     * Adds a new syntax rule or overrides existing one.
     * @param syntaxRule
     */
    public void add(SyntaxRule syntaxRule) {
        this.syntax.put(syntaxRule.getFieldName(), syntaxRule);
    }

    /**
     * Returns all syntax rules.
     * @return
     */
    public HashMap<String, SyntaxRule> getSyntax() {
        return this.syntax;
    }

    /**
     * Returns the rule for the field.
     * @param fieldName
     * @return
     */
    public SyntaxRule get(String fieldName) {
        return this.syntax.get(fieldName);
    }

    /**
     * Returns a simple example pattern of the syntax.
     * @return
     */
    public String getAsString() {

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("{");

        for(Map.Entry<String, SyntaxRule> entry : this.syntax.entrySet()) {
            stringBuilder.append(String.format(
                    "\"%s\": %s, ",
                    entry.getKey(),
                    entry.getValue().getDataType().getSyntax()
            ));
        }

        int index = stringBuilder.toString().lastIndexOf(",");
        stringBuilder.replace(index, index + 2, "");

        stringBuilder.append("}");

        return stringBuilder.toString();

    }

    /**
     * Returns a list with the name of all missing fields.
     * @param object
     * @return
     */
    public List<String> missingFields(JsonObject object) {

        List<String> fields = new ArrayList<>();

        for(SyntaxRule syntaxRule : this.syntax.values()) {
            if(!(object.has(syntaxRule.getFieldName()))) {
                fields.add(syntaxRule.getFieldName());
            }
        }

        return fields;

    }

    public boolean validate(final Field field) {

        SyntaxRule syntaxRule = this.syntax.get(field.getName());

        if(syntaxRule == null) {
            return false;
        }

        if(!(syntaxRule.isNullable())) {

            if(field.getValue() == null) {
                return false;
            }

        }

        if(syntaxRule.isAutoincrement()) {

            try {
                if (field.as(DataTypes.INTEGER) == null || field.as(DataTypes.LONG) == null) {
                    return false;
                }
            } catch(Exception e) {
                return false;
            }

        }

        if(syntaxRule.isUnique() || syntaxRule.isAutoincrement()) {

            if(this.table.findFirst(new Query() {
                @Override
                public boolean matches(DataSet dataSet) {
                    return dataSet.get(field.getName()).get().getValue().equals(field.getValue());
                }
            }).isPresent()) {
                return false;
            }

        }

        return true;

    }

    /**
     * Checks the data set against the defined syntax rules.
     * @param dataSet
     * @return
     */
    public boolean validate(final DataSet dataSet) {

        for(final Map.Entry<String, SyntaxRule> entry : this.syntax.entrySet()) {

            if(!(dataSet.get(entry.getKey()).isPresent()) && !(entry.getValue().isAutoincrement())) {
                return false;
            }

            if(!(entry.getValue().isNullable()) && !(entry.getValue().isAutoincrement())) {

                if(dataSet.get(entry.getKey()).get().getValue() == null) {
                    return false;
                }

            }

            if(entry.getValue().isAutoincrement() && !(dataSet.get(entry.getKey()).isPresent())) {

                Object aiValue = 0;

                if(!(this.table.all().isEmpty())) {

                    if(entry.getValue().getDataType() == DataTypes.INTEGER) {
                        DataSet max = this.table.all().get(0);

                        for(DataSet e : this.table.all()) {

                            if((int)max.get(entry.getKey()).get().getValue() < (int) e.get(entry.getKey()).get().getValue()) {
                                max = e;
                            }

                        }

                        aiValue = (int) max.get(entry.getKey()).get().getValue() + 1;
                    }

                    if(entry.getValue().getDataType() == DataTypes.LONG) {
                        DataSet max = this.table.all().get(0);

                        for(DataSet e : this.table.all()) {

                            if((long)max.get(entry.getKey()).get().getValue() < (long) e.get(entry.getKey()).get().getValue()) {
                                max = e;
                            }

                        }

                        aiValue = (long) max.get(entry.getKey()).get().getValue() + 1;
                    }

                }

                dataSet.addField(new Field(
                        this.table,
                        entry.getKey(),
                        aiValue
                ));

            }

            if(entry.getValue().isUnique()) {

                if(this.table.findFirst(new Query() {
                    @Override
                    public boolean matches(DataSet checkSet) {
                        return checkSet.get(entry.getKey()).get().getValue().equals(dataSet.get(entry.getKey()).get().getValue());
                    }
                }).isPresent()) {
                    return false;
                }

            }

        }

        return true;

    }

    /**
     * Validates the JsonObject against the syntax rules.
     * @param object
     * @return
     */
    public boolean parseValidation(final JsonObject object) {

        //TODO improve validation

        if(object.isJsonNull() || !(object.isJsonObject())) {
            return false;
        }

        for(final Map.Entry<String, SyntaxRule> entry : this.syntax.entrySet()) {

            if(!(object.has(entry.getKey())) && !(entry.getValue().isAutoincrement())) {
                return false;
            }

            if(!(entry.getValue().isNullable()) && !(entry.getValue().isAutoincrement())) {

                if(object.get(entry.getKey()).isJsonNull()) {
                    return false;
                }

            }

            if(entry.getValue().isUnique()) {

                if(this.table.findFirst(new Query() {
                    @Override
                    public boolean matches(DataSet checkSet) {
                        return checkSet.get(entry.getKey()).get().getValue().equals(object.get(entry.getKey()).getAsString());
                    }
                }).isPresent()) {
                    return false;
                }

            }

        }

        return true;

    }

}
