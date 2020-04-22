package gofabian.db;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;

public class BookRecord extends UpdatableRecordImpl<BookRecord> implements Record2<Long, String> {

    public BookRecord() {
        super(BookTable.BOOK_TABLE);
    }

    @Override
    public Field<Long> field1() {
        return BookTable.BOOK_TABLE.ID;
    }

    @Override
    public Field<String> field2() {
        return BookTable.BOOK_TABLE.NAME;
    }

    @Override
    public Long value1() {
        return (Long) get(0);
    }

    @Override
    public String value2() {
        return (String) get(1);
    }

    @Override
    public BookRecord value1(Long value) {
        set(0, value);
        return this;
    }

    @Override
    public BookRecord value2(String value) {
        set(1, value);
        return this;
    }

    @Override
    public BookRecord values(Long value1, String value2) {
        value1(value1);
        value2(value2);
        return this;
    }

    @Override
    public Long component1() {
        return value1();
    }

    @Override
    public String component2() {
        return value2();
    }

    @Override
    public Row2<Long, String> fieldsRow() {
        //noinspection unchecked
        return (Row2<Long, String>) super.fieldsRow();
    }

    @Override
    public Row2<Long, String> valuesRow() {
        //noinspection unchecked
        return (Row2<Long, String>) super.valuesRow();
    }

    @Override
    public Record1<Long> key() {
        //noinspection unchecked
        return (Record1<Long>) super.key();
    }

}
