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
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRule;
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRuleBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by Yonas on 29.12.2015.
 */
public class Table {

    /**
     * the number of threads in the pool
     */
    private static final int THREADS = 10;

    private SweetDB sweetDB;
    private ExecutorService executorService = Executors.newFixedThreadPool(Table.THREADS);

    private final File path;
    private final String name;

    private Syntax syntax = new Syntax(this);

    private List<DataSet> dataSets = new ArrayList<>();

    public Table(SweetDB sweetDB, File path) {
        this.sweetDB = sweetDB;
        this.path = path;
        this.name = path.getName().substring(0, path.getName().indexOf("."));
    }

    public Table(SweetDB sweetDB, File path, List<SyntaxRule> syntaxRules) {
        this(sweetDB, path);
        syntaxRules.forEach(rule -> this.syntax.add(rule));
    }

    public SweetDB getDatabase() {
        return this.sweetDB;
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
    public void insert(DataSet dataSet) {
        this.dataSets.add(dataSet);

        if(this.sweetDB.isAutosave()) {
            this.store();
        }
    }

    /**
     * Finds a list of DataSets in the table.
     * @param predicate
     * @return
     */
    public List<DataSet> find(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).collect(Collectors.toCollection(ArrayList::new));

    }

    public Optional<DataSet> findAny(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).findAny();

    }

    public Optional<DataSet> findFirst(Predicate<? super DataSet> predicate) {

        return this.dataSets.stream().filter(predicate).findFirst();

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

        Gson gson = new Gson();

        try {
            JsonObject head = gson.fromJson(data, JsonObject.class);
            Optional<JsonObject> table = Optional.empty();

            if(head.has("table")) {
                table = Optional.of(head.get("table").getAsJsonObject());
            }

            if(table.get().has("syntax")) {

                JsonArray syntaxRules = table.get().get("syntax").getAsJsonArray();

                syntaxRules.forEach(entry -> {

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

                });


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

                        System.out.println(entry.getAsJsonObject().entrySet().isEmpty());

                        List<Field> fields = entry.getAsJsonObject().entrySet().stream().map(field -> new Field(this.sweetDB, this, field.getKey(), this.syntax.get(field.getKey()).getDataType().parse((field.getValue().isJsonNull() ? null : field.getValue().getAsString())))).collect(Collectors.toList());

                        this.dataSets.add(new DataSet(fields));

                    });

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
            this.executorService = Executors.newFixedThreadPool(10);
        }

        Future<?> task = this.executorService.submit(() -> {

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
