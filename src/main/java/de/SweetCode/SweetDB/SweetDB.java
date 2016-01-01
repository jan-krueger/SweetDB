package de.SweetCode.SweetDB;

import de.SweetCode.SweetDB.Table.Action.CreateTableAction;
import de.SweetCode.SweetDB.Table.Table;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by Yonas on 29.12.2015.
 */
public class SweetDB {

    private final File path;
    private final File parityPath;

    private boolean autosave = false;
    private int storageThreads = 10;

    private boolean debugging = false;

    private final List<Table> tables = new ArrayList<>();

    public SweetDB(String path, String... tables) {
        this.path = new File(path);
        this.parityPath = new File(this.path.getPath() + File.separator + "parity");

        for(String table : tables) {
            this.tables.add(new Table(this, new File(this.path.getPath() + File.separator + table + ".sweet")));
        }
    }

    public SweetDB(String path, boolean autosave, String... tables) {
        this(path, tables);
        this.autosave = autosave;
    }

    /**
     * Returns the path to the root of database.
     * @return
     */
    public File getPath() {
        return this.path;
    }

    /**
     * Returns all tables.
     * @return
     */
    public List<Table> getTables() {
        return this.tables;
    }

    /**
     * Returns true if the debugging modus is active.
     * @return
     */
    public boolean isDebugging() {
        return this.debugging;
    }

    public int getStorageThreads() {
        return this.storageThreads;
    }

    public boolean isAutosave() {
        return this.autosave;
    }

    public void addTable(Table table) {

        this.tables.add(table);

    }

    public boolean tableExist(String tableName) {

        return this.table(tableName).isPresent();

    }

    public Optional<Table> table(String tableName) {

        return this.tables.stream().filter(table -> table.getName().equals(tableName)).findFirst();

    }

    public CreateTableAction createTable() {
        return new CreateTableAction(this);
    }

    public void debugging(boolean debugging) {
        this.debugging = debugging;
    }

    public void storageThreads(int storageThreads) {
        this.storageThreads = storageThreads;
    }

    public void store() {

        this.tables.forEach(table -> table.store());

    }

    public void load() throws IOException {

        if(!(this.path.exists())) {
            throw new IllegalArgumentException(String.format(
                    "Can't load the database. The path \"%s\" doesn't exist.",
                    this.path.getPath()
            ));
        }

        Iterator<Table> iterator = this.tables.iterator();
        while(iterator.hasNext()) {

            Table table = iterator.next();

            if(!(table.getPath().exists())) {
                iterator.remove();
                continue;
            }

            int typePos = table.getPath().getPath().lastIndexOf(".");
            Optional<String> extension = Optional.empty();
            if(typePos > 0) {
                extension = Optional.of(table.getPath().toString().substring(typePos + 1));
            }

            if(!(extension.isPresent())) {
                table.getPath().createNewFile();
            }

            if(extension.isPresent() && !(extension.get().equals("sweet"))) {
                if(this.isDebugging()) {
                    throw new FileNotFoundException(String.format(
                            "SweetDB couldn't find the file \"%s\" but the file has the wrong extension \"%s\". The file has the wrong file-extension or you didn't create the table yet.",
                            table.getPath().toString().substring(0, typePos - 1) + ".sweet",
                            extension
                    ));
                }

                continue;
            }

            LineIterator lineIterator = FileUtils.lineIterator(table.getPath(), "UTF-8");

            Optional<String> raw = Optional.empty();

            try {

                StringBuilder stringBuilder = new StringBuilder();

                lineIterator.forEachRemaining(line -> stringBuilder.append(line));
                raw = Optional.of(stringBuilder.toString());

            } finally {

                LineIterator.closeQuietly(lineIterator);

            }

            if(raw.isPresent()) {
                table.parse(raw.get());
            }

        }

    }

}
