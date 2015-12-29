package de.SweetCode.SweetDB.Table.Syntax;


import com.google.gson.JsonObject;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataType.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Syntax {

    private HashMap<String, DataType> syntax = new HashMap<>();

    public Syntax() {}

    public void add(String fieldName, DataType dataType) {
        this.syntax.put(fieldName, dataType);
    }

    public HashMap<String, DataType> getSyntax() {
        return this.syntax;
    }

    public DataType get(String fieldName) {
        return this.syntax.get(fieldName);
    }

    public String getAsString() {

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("{");

        for(Map.Entry<String, DataType> entry : this.syntax.entrySet()) {
            stringBuilder.append(String.format(
                    "\"%s\": %s, ",
                    entry.getKey(),
                    entry.getValue().getSyntax()
            ));
        }

        int index = stringBuilder.toString().lastIndexOf(",");
        stringBuilder.replace(index, index + 2, "");

        stringBuilder.append("}");

        return stringBuilder.toString();

    }

    public List<String> missingFields(JsonObject object) {

        return this.syntax.entrySet().stream().filter(entry -> !(object.has(entry.getKey()))).map(Map.Entry::getKey).collect(Collectors.toList());

    }

    public boolean validate(DataSet dataSet) {

        for(Map.Entry<String, DataType> entry : this.syntax.entrySet()) {

            if(!(dataSet.get(entry.getKey()).isPresent())) {
                return false;
            }

        }

        return true;

    }

    public boolean validate(JsonObject object) {

        //TODO improve validation

        if(object.isJsonNull() || !(object.isJsonObject())) {
            return false;
        }

        for(Map.Entry<String, DataType> entry : this.syntax.entrySet()) {

            if(!(object.has(entry.getKey()))) {
                return false;
            }

        }

        return true;

    }

}
