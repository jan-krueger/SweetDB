package de.SweetCode.SweetDB.Table;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.deploy.util.StringUtils;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Action.InsertAction;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Table {

    private SweetDB sweetDB;

    private final File path;
    private final String name;

    private Syntax syntax = new Syntax();

    private List<DataSet> dataSets = new ArrayList<>();

    public Table(SweetDB sweetDB, File path) {
        this.sweetDB = sweetDB;
        this.path = path;
        this.name = path.getName().substring(0, path.getName().indexOf("."));
    }

    public File getPath() {
        return this.path;
    }

    public String getName() {
        return this.name;
    }

    public void store() {

        JsonObject head = new JsonObject();

        JsonObject table = new JsonObject();
        JsonObject syntax = new JsonObject();

        for(Map.Entry<String, DataType> entry : this.syntax.getSyntax().entrySet()) {
            syntax.addProperty(entry.getKey(), entry.getValue().getName());
        }

        JsonArray data = new JsonArray();
        for(DataSet entry : this.dataSets) {

            JsonObject dataEntry = new JsonObject();
            for(Field field : entry.getFields()) {
                dataEntry.addProperty(field.getName(), field.getValue().toString());
            }

            data.add(dataEntry);

        }

        table.add("syntax", syntax);
        head.add("table", table);
        head.add("data", data);

        try {

            if(!(this.path.exists())) {
                this.path.createNewFile();
            }

            FileUtils.write(this.path, head.toString(), "UTF-8", false);
        } catch (IOException e) {
            e.printStackTrace(); //TODO
        }

    }

    public InsertAction insert() {
        return new InsertAction(this, this.syntax);
    }

    public void insert(DataSet dataSet) {
        this.dataSets.add(dataSet);

        if(this.sweetDB.isAutosave()) {
            this.store();
        }
    }

    public List<DataSet> find(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).collect(Collectors.toCollection(ArrayList::new));

    }

    public Optional<DataSet> findAny(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).findAny();

    }

    public Optional<DataSet> findFirst(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).findFirst();

    }

    public List<DataSet> all() {
        return this.dataSets;
    }

    public void parse(String data) {

        Gson gson = new Gson();

        try {
            JsonObject head = gson.fromJson(data, JsonObject.class);
            Optional<JsonObject> table = Optional.empty();

            if(head.has("table")) {
                table = Optional.of(head.get("table").getAsJsonObject());
            }

            if(table.get().has("syntax")) {

                JsonObject syntax = table.get().get("syntax").getAsJsonObject();

                for(Map.Entry<String, JsonElement> entry : syntax.entrySet()) {

                    Optional<DataType> dataType = DataTypes.get(entry.getValue().getAsString());

                    if(dataType.isPresent()) {

                        this.syntax.add(entry.getKey(), dataType.get());

                    } else {
                        throw new IllegalArgumentException(String.format(
                            "\"%s\" is not a valid DataType. Please check the syntax of \"%s\".",
                            entry.getValue().getAsString(),
                            this.getName()
                        ));
                    }

                }

                if(head.has("data") && !(this.syntax.getSyntax().isEmpty())) {

                    JsonArray dataSets = head.get("data").getAsJsonArray();

                    dataSets.forEach(entry -> {

                        if(!(this.syntax.validate(entry.getAsJsonObject()))) {
                            throw new IllegalArgumentException(String.format(
                                    "Invalid syntax in \"%s\" -> %s expected %s. Missing field(s): %s",
                                    this.getName(),
                                    entry.toString(),
                                    this.syntax.getAsString(),
                                    StringUtils.join(this.syntax.missingFields(entry.getAsJsonObject()), ", ")
                            ));
                        }

                        List<Field> fields = entry.getAsJsonObject().entrySet().stream().map(field -> new Field(field.getKey(), this.syntax.get(field.getKey()).parse(field.getValue().getAsString()))).collect(Collectors.toList());

                        this.dataSets.add(new DataSet(fields));

                    });

                }
            }

        } catch (Exception exception) {
            exception.printStackTrace();
            //TODO invalid syntax
        }


    }

}
