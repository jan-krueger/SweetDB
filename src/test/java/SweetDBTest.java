import de.SweetCode.SweetDB.DataSet.DataSet;
import de.SweetCode.SweetDB.DataSet.Field;
import de.SweetCode.SweetDB.SweetDB;
import de.SweetCode.SweetDB.Table.Table;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;

/**
 * Created by Yonas on 29.12.2015.
 */
public class SweetDBTest {

    public static void main(String[] args) {

        SweetDB sweetDB = new SweetDB("F:\\SweetDB", "users");
        try {
            sweetDB.load();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Table users = sweetDB.table("users").get();

        long time = System.currentTimeMillis();
        for(int i = 0; i < 100000; i++) {
            users.insert()
                    .add("id", i)
                    .add("name", "A")
                    .add("active", true)
                    .add("time", Timestamp.from(Instant.now()))
                    .build();
        }
        users.store();

        System.out.println("Time to STORE 10000 items: " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        try {
            sweetDB.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Time to LOAD 10000 items: " + (System.currentTimeMillis() - time));

    }

}
