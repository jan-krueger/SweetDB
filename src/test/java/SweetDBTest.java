import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataType.DataTypes;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Syntax.SyntaxRuleBuilder;
import de.SweetCode.SweetDB.Table.Table;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * Created by Yonas on 29.12.2015.
 */
public class SweetDBTest {

    public static void main(String[] args) {

        SweetDB sweetDB = new SweetDB("F:\\SweetDB", "users");
        sweetDB.debugging(true);
        sweetDB.storageThreads(10);
        try {
            sweetDB.load();
        } catch (IOException e) {
            e.printStackTrace();
        }

        sweetDB.createTable()
            .name("users")
            .overrideExisting(false)
                .add(
                    SyntaxRuleBuilder.create()
                            .fieldName("id")
                            .isUnique(true)
                            .isAutoincrement(true)
                            .dataType(DataTypes.INTEGER)
                        .build()
                    )
                    .add(
                        SyntaxRuleBuilder.create()
                            .fieldName("name")
                            .isNullable(true)
                            .dataType(DataTypes.STRING)
                        .build()
                    )
                    .add(
                            SyntaxRuleBuilder.create()
                                    .fieldName("active")
                                    .dataType(DataTypes.BOOLEAN)
                                    .build()
                    )
                    .add(
                            SyntaxRuleBuilder.create()
                                    .fieldName("time")
                                    .dataType(DataTypes.TIMESTAMP)
                                    .build()
                    )
        .build();

        Table users = sweetDB.table("users").get();

        users.insert()
                .add("name", "Jonas")
                .add("active", true)
                .add("time", Timestamp.from(Instant.now()))
                .build();

        Optional<DataSet> result = users.findFirst(dataSet -> {

            if (dataSet.get("name").get().getValue().equals("Jonas")) {
                return true;
            }

            return false;

        });

        if(result.isPresent()) {
            result.get().get("time").get().update(Timestamp.from(Instant.now()));
        }

        sweetDB.store();

    }

}
