package de.SweetCode.SweetDB.Table.Syntax;

import de.SweetCode.SweetDB.DataType.DataType;

/**
 * Created by Yonas on 30.12.2015.
 */
public class SyntaxRule {

    private String fieldName;
    private DataType dataType;

    private boolean isUnique = false;
    private boolean isNullable = false;
    private boolean isAutoincrement = false;

    public SyntaxRule(String fieldName, DataType dataType, boolean isUnique, boolean isNullable, boolean isAutoincrement) {
        this.fieldName = fieldName;
        this.dataType = dataType;

        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.isAutoincrement = isAutoincrement;
    }

    public static SyntaxRuleBuilder builder() {
        return SyntaxRuleBuilder.create();
    }

    public String getFieldName() {
        return fieldName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public boolean isAutoincrement() {
        return isAutoincrement;
    }

}
