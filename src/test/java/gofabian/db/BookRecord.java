package gofabian.db;

import org.jooq.*;
import org.jooq.impl.UpdatableRecordImpl;

import java.time.LocalDateTime;

public class BookRecord extends UpdatableRecordImpl<BookRecord> implements Record3<Long, String, LocalDateTime> {

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
    public Field<LocalDateTime> field3() {
        return BookTable.BOOK_TABLE.TIMESTAMP;
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
    public LocalDateTime value3() {
        return (LocalDateTime) get(2);
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
    public BookRecord value3(LocalDateTime value) {
        set(2, value);
        return this;
    }

    @Override
    public BookRecord values(Long value1, String value2, LocalDateTime value3) {
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
    public String component2() {
        return value2();
    }

    @Override
    public LocalDateTime component3() {
        return value3();
    }

    @Override
    public Row3<Long, String, LocalDateTime> fieldsRow() {
        //noinspection unchecked
        return (Row3<Long, String, LocalDateTime>) super.fieldsRow();
    }

    @Override
    public Row3<Long, String, LocalDateTime> valuesRow() {
        //noinspection unchecked
        return (Row3<Long, String, LocalDateTime>) super.valuesRow();
    }

    @Override
    public Record1<Long> key() {
        //noinspection unchecked
        return (Record1<Long>) super.key();
    }

}
