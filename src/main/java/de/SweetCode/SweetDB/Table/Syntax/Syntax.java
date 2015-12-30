package de.SweetCode.SweetDB.Table.Syntax;


import com.google.gson.JsonObject;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
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
                    return false;
                }

            }

            if(entry.getValue().isAutoincrement()) {

                int aiValue = 0;

                if(!(this.table.all().isEmpty())) {
                    aiValue = (int) this.table.all().get(
                            this.table.all().size() - 1
                    ).get(entry.getKey()).get().getValue() + 1;
                }

                if(dataSet.get(entry.getKey()).isPresent()) {
                    dataSet.get(entry.getKey()).get().update(aiValue);
                } else {
                    dataSet.addField(new Field(
                            this.table.getDatabase(),
                            this.table,
                            entry.getKey(),
                            aiValue
                    ));
                }

            }

            if(entry.getValue().isUnique()) {

                if(!(this.table.find(content -> content.get(entry.getKey()).get().getValue().equals(dataSet.get(entry.getKey()).get().getValue())).isEmpty())) {
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
    public boolean validate(JsonObject object) {

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
                    System.out.println(entry.getKey());
                    System.out.println(object.get(entry.getKey()));
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
