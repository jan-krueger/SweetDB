package de.SweetCode.SweetDB.Table.Syntax;

import de.SweetCode.SweetDB.DataType.DataType;
import de.SweetCode.SweetDB.DataType.DataTypes;

import java.util.Optional;

/**
 * Created by Yonas on 30.12.2015.
 */
public class SyntaxRuleBuilder {

    private Optional<String> fieldName = Optional.empty();
    private Optional<DataType> dataType = Optional.empty();

    private boolean isUnique = false;
    private boolean isNullable = false;
    private boolean isAutoincrement = false;

    public SyntaxRuleBuilder() {}

    public static SyntaxRuleBuilder create() {
        return new SyntaxRuleBuilder();
    }

    public SyntaxRuleBuilder fieldName(String fieldName) {
        this.fieldName = Optional.of(fieldName);
        return this;
    }

    public SyntaxRuleBuilder dataType(DataType dataType) {
        this.dataType = Optional.of(dataType);
        return this;
    }

    public SyntaxRuleBuilder isUnique(boolean isUnique) {
        this.isUnique = isUnique;
        return this;
    }

    public SyntaxRuleBuilder isNullable(boolean isNullable) {
        this.isNullable = isNullable;
        return this;
    }

    public SyntaxRuleBuilder isAutoincrement(boolean isAutoincrement) {
        this.isAutoincrement = isAutoincrement;
        return this;
    }

    public SyntaxRule build() {

        if(!(this.fieldName.isPresent())) {
            throw new IllegalArgumentException("You have to provide a field name.");
        }

        if(!(this.dataType.isPresent())) {
            throw new IllegalArgumentException("You have to provide a DataType.");
        }

        if(this.isAutoincrement) {

            if(!(this.dataType.get().equals(DataTypes.INTEGER)) && !(this.dataType.get().equals(DataTypes.LONG))) {
                throw new IllegalArgumentException("Only DataTypes.INTEGER and DataTypes.LONG fields can be flagged as auto incrementing.");
            }

        }

        return new SyntaxRule(
                this.fieldName.get(),
                this.dataType.get(),
                this.isUnique,
                this.isNullable,
                this.isAutoincrement
        );

    }

}
