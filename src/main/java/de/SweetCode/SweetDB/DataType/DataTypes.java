package de.SweetCode.SweetDB.DataType;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by Yonas on 29.12.2015.
 */
public enum DataTypes implements DataType {


    BOOLEAN {

        @Override
        public String getName() {
            return "boolean";
        }

        @Override
        public String getSyntax() {
            return "true";
        }

        @Override
        public Boolean parse(String value) {
            return Boolean.parseBoolean(value);
        }

    },
    BYTE {

        @Override
        public String getName() {
            return "byte";
        }

        @Override
        public String getSyntax() {
            return "-127";
        }

        @Override
        public Byte parse(String value) {
            return Byte.parseByte(value);
        }

    },
    TIMESTAMP {

        @Override
        public String getName() {
            return "timestamp";
        }

        @Override
        public String getSyntax() {
            return "2015-11-21 13:45:00";
        }

        @Override
        public Timestamp parse(String value) {
            return java.sql.Timestamp.valueOf(value);
        }

    },
    DOUBLE {

        @Override
        public String getName() {
            return "double";
        }

        @Override
        public String getSyntax() {
            return "43.43";
        }

        @Override
        public Double parse(String value) {
            return Double.parseDouble(value);
        }

    },
    FLOAT {

        @Override
        public String getName() {
            return "float";
        }

        @Override
        public String getSyntax() {
            return "43.432698";
        }

        @Override
        public Float parse(String value) {
            return Float.parseFloat(value);
        }

    },
    INTEGER {

        @Override
        public String getName() {
            return "integer";
        }

        @Override
        public String getSyntax() {
            return "43";
        }

        @Override
        public Integer parse(String value) {
            return Integer.parseInt(value);
        }

    },
    LONG {

        @Override
        public String getName() {
            return "long";
        }

        @Override
        public String getSyntax() {
            return "572454";
        }

        @Override
        public Long parse(String value) {
            return Long.parseLong(value);
        }

    },
    SHORT {

        @Override
        public String getName() {
            return "short";
        }

        @Override
        public String getSyntax() {
            return "5";
        }

        @Override
        public Short parse(String value) {
            return Short.parseShort(value);
        }

    },
    STRING {

        @Override
        public String getName() {
            return "string";
        }

        @Override
        public String getSyntax() {
            return "\"Value\"";
        }

        @Override
        public String parse(String value) {
            return value;
        }



    },
    UUID {

        @Override
        public String getName() {
            return "uuid";
        }

        @Override
        public String getSyntax() {
            return "\"78238609-cab0-4882-b9ba-9606f65548cd\"";
        }

        @Override
        public UUID parse(String value) {
            return java.util.UUID.fromString(value);
        }

    };

    public static Optional<DataType> get(String name) {

        for(DataTypes entry : values()) {

            if(entry.getName().equals(name)) { //TODO maybe equalsIgnoreCase instead of equals
                return Optional.of(entry);
            }

        }

        return Optional.empty();

    }

}
