package de.SweetCode.Test.SweetDB;

import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.Query;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Table;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Created by Yonas on 01.01.2016.
 */
public class SweetDBTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String databasePath;
    private SweetDB database;

    @Before
    public void createDatabase() throws IOException {

        this.databasePath = this.temporaryFolder.getRoot().getPath();
        File file = this.temporaryFolder.newFile("users-mockup.sweet");
        FileUtils.write(file,
                "{\"table\":{\"syntax\":[{\"field\":\"id\",\"dataType\":\"integer\",\"isNullable\":false,\"isUnique\":true,\"isAutoincrement\":true},{\"field\":\"name\",\"dataType\":\"string\",\"isNullable\":false,\"isUnique\":false,\"isAutoincrement\":false},{\"field\":\"active\",\"dataType\":\"boolean\",\"isNullable\":false,\"isUnique\":false,\"isAutoincrement\":false}]},\"data\":[{\"name\":\"Jan\",\"active\":\"true\",\"id\":\"0\"},{\"name\":\"Jonas\",\"active\":\"true\",\"id\":\"1\"}]}",
                "UTF-8",
                false
        );


        this.database = new SweetDB(this.databasePath, "users-mockup");
        this.database.debugging(true);
        this.database.storageThreads(2);
        this.database.load();

    }

    @Test
    public void testSweetDBConstructorWithValidData() {

        SweetDB databaseA = new SweetDB(this.databasePath);

        Assert.assertEquals(databaseA.isAutosave(), false);
        Assert.assertEquals(databaseA.isDebugging(), false);

        databaseA.debugging(true);
        Assert.assertEquals(databaseA.isDebugging(), true);

        SweetDB databaseB = new SweetDB(this.databasePath, true);

        Assert.assertEquals(databaseB.isAutosave(), true);
        Assert.assertEquals(databaseB.isDebugging(), false);

    }

    @Test
    public void testLoadMethodWithValidPath() throws IOException {

        this.database.load();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadMethodWithInvalidPath() throws IOException {

        SweetDB database = new SweetDB("", "users-mockup");
        database.load();

    }

    @Test
    public void testGetPathMethod() {

        Assert.assertEquals(this.database.getPath().getPath(), this.temporaryFolder.getRoot().getPath());

    }

    @Test
    public void testGetTablesMethod() {

        Assert.assertEquals(this.database.getTables().size(), 1);

    }

    @Test
    public void testGetStorageThreadsMethod() {

        Assert.assertEquals(this.database.getStorageThreads(), 2);

    }

    @Test
    public void testAddTableMethod() {

        Assert.assertEquals(
                this.database.addTable(null),
                false
        );

        Assert.assertEquals(
                this.database.addTable(
                        this.database.table("users-mockup").get()
                ),
                false
        );

    }

    @Test
    public void testTableMethod() {

        Assert.assertEquals(
                this.database.table("users-mockup").isPresent(),
                true
        );

    }

    @Test
    public void testFindMethods() {

        Table table = this.database.table("users-mockup").get();

        Assert.assertEquals(
                table.find(new Query() {
                    @Override
                    public boolean matches(DataSet dataSet) {
                        return dataSet.get("name").get().getValue().equals("Jonas");
                    }
                }).isEmpty(),
                false
        );

        Assert.assertEquals(
                table.findFirst(new Query() {
                    @Override
                    public boolean matches(DataSet dataSet) {
                        return dataSet.get("name").get().getValue().equals("Jonas");
                    }
                }).isPresent(),
                true
        );

    }

    @Test
    public void testDataSet() {

        Table table = this.database.table("users-mockup").get();

        DataSet dataSet = table.findFirst(new Query() {
            @Override
            public boolean matches(DataSet dataSet) {
                return dataSet.get("name").get().getValue().equals("Jan");
            }
        }).get();

        Assert.assertEquals(dataSet.getFields().size(), 3);

        Assert.assertEquals(dataSet.delete(), true);

    }

    @Test
    public void testInsert() {

        Table table = this.database.table("users-mockup").get();
        boolean value = table.insert()
                .add("name", "Yonas")
                .add("active", true)
                .build();

        Assert.assertEquals(value, true);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInsert() {

        Table table = this.database.table("users-mockup").get();
        table.insert()
            .add("active", false)
        .build();

    }

    @Test
    public void testField() {

        Table table = this.database.table("users-mockup").get();

        DataSet dataSet = table.findFirst(new Query() {
            @Override
            public boolean matches(DataSet dataSet) {
                return dataSet.get("name").get().getValue().equals("Jan");
            }
        }).get();

        Field field = dataSet.get("name").get();
        Assert.assertEquals(field.getName(), "name");
        Assert.assertEquals(field.getValue().toString(), "Jan");
        Assert.assertEquals(field.as(DataTypes.STRING).getClass().isAssignableFrom(String.class), true);
        Assert.assertEquals(field.update("m0ys"), true);

    }

    @Test
    public void testStoreAndDrop() {

        Table table = this.database.table("users-mockup").get();
        table.store();

        table.drop();

        Assert.assertEquals(this.database.table("users-mockup").isPresent(), false);

    }

}
