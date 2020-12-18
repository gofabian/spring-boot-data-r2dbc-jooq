package gofabian.example;

import org.jooq.*;
import org.jooq.impl.UpdatableRecordImpl;

public class JsonRecord extends UpdatableRecordImpl<JsonRecord> implements Record3<Long, JSON, JSONB> {

    public JsonRecord() {
        super(JsonTable.JSON_TABLE);
    }

    @Override
    public Field<Long> field1() {
        return JsonTable.JSON_TABLE.ID;
    }

    @Override
    public Field<JSON> field2() {
        return JsonTable.JSON_TABLE.JSON;
    }

    @Override
    public Field<JSONB> field3() {
        return JsonTable.JSON_TABLE.JSONB;
    }

    @Override
    public Long value1() {
        return (Long) get(0);
    }

    @Override
    public JSON value2() {
        return (JSON) get(1);
    }

    @Override
    public JSONB value3() {
        return (JSONB) get(2);
    }

    @Override
    public JsonRecord value1(Long value) {
        set(0, value);
        return this;
    }

    @Override
    public JsonRecord value2(JSON value) {
        set(1, value);
        return this;
    }

    @Override
    public JsonRecord value3(JSONB value) {
        set(2, value);
        return this;
    }

    @Override
    public JsonRecord values(Long value1, JSON value2, JSONB value3) {
        value1(value1);
        value2(value2);
        value3(value3);
        return this;
    }

    @Override
    public Long component1() {
        return value1();
    }

    @Override
    public JSON component2() {
        return value2();
    }

    @Override
    public JSONB component3() {
        return value3();
    }

    @Override
    public Row3<Long, JSON, JSONB> fieldsRow() {
        //noinspection unchecked
        return (Row3<Long, JSON, JSONB>) super.fieldsRow();
    }

    @Override
    public Row3<Long, JSON, JSONB> valuesRow() {
        //noinspection unchecked
        return (Row3<Long, JSON, JSONB>) super.valuesRow();
    }

    @Override
    public Record1<Long> key() {
        //noinspection unchecked
        return (Record1<Long>) super.key();
    }

}
