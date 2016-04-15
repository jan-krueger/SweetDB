package de.SweetCode.SweetDB.Table;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.Optional;
import de.SweetCode.SweetDB.Query;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Action.InsertAction;
import de.SweetCode.SweetDB.Table.Syntax.Syntax;
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRule;
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRuleBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Table {

    private SweetDB sweetDB;
    private ExecutorService executorService = null;

    private final File path;
    private final String name;

    private String originalData = null;

    private Syntax syntax = new Syntax(this);

    private List<DataSet> dataSets = new ArrayList<>();

    public Table(SweetDB sweetDB, File path) {
        this.sweetDB = sweetDB;
        this.path = path;
        this.name = path.getName().substring(0, path.getName().indexOf("."));
        this.executorService = Executors.newFixedThreadPool(this.sweetDB.getStorageThreads());
    }

    public Table(SweetDB sweetDB, File path, List<SyntaxRule> syntaxRules) {
        this(sweetDB, path);
        for(SyntaxRule entry : syntaxRules) this.syntax.add(entry);
    }

    public SweetDB getDatabase() {
        return this.sweetDB;
    }

    public Syntax getSyntax() {
        return this.syntax;
    }

    /**
     * The path to the file of the table.
     * @return
     */
    public File getPath() {
        return this.path;
    }

    /**
     * The name of the table.
     * @return
     */
    public String getName() {
        return this.name;
    }

    /**
     * Creates a new InsertAction to insert a new DataSet into the table.
     * @return
     */
    public InsertAction insert() {
        return new InsertAction(this.sweetDB, this, this.syntax);
    }

    /**
     * Inserts a DataSet into the table.
     * @param dataSet
     */
    public boolean insert(DataSet dataSet) {

        if(!(this.syntax.validate(dataSet))) {
            if(this.sweetDB.isDebugging()) {
                throw new IllegalArgumentException(String.format(
                        "Invalid insert query. Syntax: %s",
                        this.syntax.getAsString()
                ));
            } else {
                return false;
            }
        }

         this.dataSets.add(dataSet);

        if(this.sweetDB.isAutosave()) {
            this.store();
        }

        return true;
    }

    public boolean update(DataSet dataSet, Query query) {

        if(!(this.syntax.validate(dataSet))) {
            if(this.sweetDB.isDebugging()) {
                throw new IllegalArgumentException(String.format(
                        "Invalid update query. Syntax: %s",
                        this.syntax.getAsString()
                ));
            } else {
                return false;
            }
        }

        for(DataSet entry : this.find(query)) {
            entry.getFields().clear();
            for(Field field : entry.getFields()) {
                entry.addField(field);
            }
        }

        return true;

    }

    /**
     * Finds a list of DataSets in the table.
     * @param query
     * @return
     */
    public List<DataSet> find(Query query) {

        List<DataSet> list = new ArrayList<>();

        for(DataSet dataSet : this.dataSets) {
            if(query.matches(dataSet)) {
                list.add(dataSet);
            }
        }

        return list;

    }

    public Optional<DataSet> findFirst(Query query) {

        for(DataSet dataSet : this.dataSets) {
            if(query.matches(dataSet)) {
                return Optional.of(dataSet);
            }
        }

        return Optional.empty();

    }

    /**
     * Returns a list with all DataSets from the table.
     * @return
     */
    public List<DataSet> all() {
        return this.dataSets;
    }

    /**
     * Drops the table.
     * @return
     */
    public void drop() {

        if(this.path.exists()) {
            this.path.delete();
        }

        Iterator<Table> tableIterator = this.sweetDB.getTables().iterator();

        while(tableIterator.hasNext()) {

            if(tableIterator.next().getName().equals(this.getName())) {
                tableIterator.remove();
                break;
            }

        }

    }

    /**
     * Parses the data.
     * @param data
     */
    public void parse(String data) {

        if(!(data == null) && data.equals(this.originalData)) {
            return;
        }

        Gson gson = new Gson();
        this.originalData = data;

        try {
            JsonObject head = gson.fromJson(data, JsonObject.class);
            Optional<JsonObject> table = Optional.empty();

            if(head.has("table")) {
                table = Optional.of(head.get("table").getAsJsonObject());
            }

            if(table.get().has("syntax")) {

                JsonArray syntaxRules = table.get().get("syntax").getAsJsonArray();

                for(JsonElement entry : syntaxRules) {

                    JsonObject syntaxRule = entry.getAsJsonObject();

                    Optional<DataType> dataType = DataTypes.get(syntaxRule.get("dataType").getAsString());

                    if(dataType.isPresent()) {

                        this.syntax.add(
                                SyntaxRuleBuilder.create()
                                        .fieldName(syntaxRule.get("field").getAsString())
                                        .dataType(dataType.get())
                                        .isUnique(syntaxRule.get("isUnique").getAsBoolean())
                                        .isNullable(syntaxRule.get("isNullable").getAsBoolean())
                                        .isAutoincrement(syntaxRule.get("isAutoincrement").getAsBoolean())
                                        .build()
                        );

                    } else {
                        throw new IllegalArgumentException(String.format(
                                "\"%s\" is not a valid DataType. Please check the syntax of \"%s\".",
                                syntaxRule.get("dataType").getAsString(),
                                this.getName()
                        ));
                    }

                }


                if(head.has("data") && !(this.syntax.getSyntax().isEmpty())) {

                    JsonArray dataSets = head.get("data").getAsJsonArray();

                    for(JsonElement entry : dataSets) {
                        if(!(this.syntax.parseValidation(entry.getAsJsonObject()))) {
                            throw new IllegalArgumentException(String.format(
                                    "Invalid syntax in \"%s\" -> %s expected %s. Missing field(s): %s",
                                    this.getName(),
                                    entry.toString(),
                                    this.syntax.getAsString(),
                                    this.syntax.missingFields(entry.getAsJsonObject()).size()
                            ));
                        }

                        List<Field> fields = new ArrayList<>();
                        for(Map.Entry<String, JsonElement> item : entry.getAsJsonObject().entrySet()) {
                            fields.add(new Field(this, item.getKey(), this.syntax.get(item.getKey()).getDataType().parse((item.getValue().isJsonNull() ? null : item.getValue().getAsString()))));
                        }

                        this.dataSets.add(new DataSet(this, fields));

                    }

                }
            }

        } catch (Exception exception) {
            exception.printStackTrace();
            //TODO invalid syntax
        }

    }

    /**
     * Stores the data of the table in the file.
     */
    public void store() {

        if(this.executorService.isShutdown()) {
            this.executorService = Executors.newFixedThreadPool(this.sweetDB.getStorageThreads());
        }

        Future<?> task = this.executorService.submit(new Runnable() {
            @Override
            public void run() {
                JsonObject headData = new JsonObject();

                JsonObject tableData = new JsonObject();
                JsonArray syntaxData = new JsonArray();

                for(Map.Entry<String, SyntaxRule> entry : syntax.getSyntax().entrySet()) {

                    JsonObject syntaxRule = new JsonObject();
                    syntaxRule.addProperty("field", entry.getKey());
                    syntaxRule.addProperty("dataType", entry.getValue().getDataType().getName());
                    syntaxRule.addProperty("isNullable", entry.getValue().isNullable());
                    syntaxRule.addProperty("isUnique", entry.getValue().isUnique());
                    syntaxRule.addProperty("isAutoincrement", entry.getValue().isAutoincrement());

                    syntaxData.add(syntaxRule);
                }

                JsonArray data = new JsonArray();
                for(DataSet entry : dataSets) {

                    JsonObject dataEntry = new JsonObject();
                    for(Field field : entry.getFields()) {
                        dataEntry.addProperty(field.getName(), (field.getValue() == null ? null : field.getValue().toString()));
                    }

                    data.add(dataEntry);

                }

                tableData.add("syntax", syntaxData);
                headData.add("table", tableData);
                headData.add("data", data);

                try {

                    if(!(path.exists())) {
                        path.createNewFile();
                    }

                    FileUtils.write(path, headData.toString(), "UTF-8", false);
                } catch (IOException e) {
                    e.printStackTrace(); //TODO
                }

            }
        });



        try {
            task.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            task.cancel(true);
        }

        this.executorService.shutdown();

    }

}
