package de.SweetCode.SweetDB.Table.Syntax;


import com.google.gson.JsonObject;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.Table.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        return this.syntax.entrySet().stream().filter(entry -> !(object.has(entry.getKey()))).map(Map.Entry::getKey).collect(Collectors.toList());

    }

    public boolean validate(Field field) {

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

            if(this.table.all().stream().filter(dataSet -> dataSet.get(field.getName()).get().getValue().equals(field.getValue())).findAny().isPresent()) {
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
    public boolean validate(DataSet dataSet) {

        for(Map.Entry<String, SyntaxRule> entry : this.syntax.entrySet()) {

            if(!(dataSet.get(entry.getKey()).isPresent()) && !(entry.getValue().isAutoincrement())) {
                return false;
            }

            if(!(entry.getValue().isNullable()) && !(entry.getValue().isAutoincrement())) {

                if(dataSet.get(entry.getKey()).get().getValue() == null) {
                    System.out.println("A");
                    return false;
                }

            }

            if(entry.getValue().isAutoincrement()) {

                Object aiValue = 0;

                if(!(this.table.all().isEmpty())) {

                    if(entry.getValue().getDataType() == DataTypes.INTEGER) {
                        for(DataSet searchValue : this.table.all()) {
                            if((int)searchValue.get(entry.getKey()).get().getValue() > (int) aiValue) {
                                aiValue = (int) searchValue.get(entry.getKey()).get().getValue() + 2;
                            }
                        }
                    }

                    if(entry.getValue().getDataType() == DataTypes.LONG) {
                        for(DataSet searchValue : this.table.all()) {
                            if((long)searchValue.get(entry.getKey()).get().getValue() > (long) aiValue) {
                                aiValue = (long) searchValue.get(entry.getKey()).get().getValue() + 2;
                            }
                        }
                    }

                    /*aiValue = (int) this.table.all().get(
                            this.table.all().size() - 1
                    ).get(entry.getKey()).get().getValue() + 1;*/
                }

                if(dataSet.get(entry.getKey()).isPresent()) {
                    dataSet.get(entry.getKey()).get().update(aiValue);
                } else {
                    dataSet.addField(new Field(
                            this.table,
                            entry.getKey(),
                            aiValue
                    ));
                }

            }

            if(entry.getValue().isUnique()) {

                if(!(this.table.find(content -> {
                    boolean v = content.get(entry.getKey()).get().getValue().equals(dataSet.get(entry.getKey()).get().getValue());
                    if(v) {
                        System.out.println(content.get(entry.getKey()).get().getValue());
                        System.out.println(dataSet.get(entry.getKey()).get().getValue());
                    }
                    return v;
                }).isEmpty())) {

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
    public boolean parseValidation(JsonObject object) {

        //TODO improve validation

        if(object.isJsonNull() || !(object.isJsonObject())) {
            return false;
        }

        for(Map.Entry<String, SyntaxRule> entry : this.syntax.entrySet()) {

            if(!(object.has(entry.getKey())) && !(entry.getValue().isAutoincrement())) {
                return false;
            }

            if(!(entry.getValue().isNullable()) && !(entry.getValue().isAutoincrement())) {

                if(object.get(entry.getKey()).isJsonNull()) {
                    return false;
                }

            }

            if(entry.getValue().isUnique()) {

                if(!(this.table.find(content -> content.get(entry.getKey()).get().getValue().equals(object.get(entry.getKey()).getAsString())).isEmpty())) {
                    return false;
                }

            }

        }

        return true;

    }

}
