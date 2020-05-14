package gofabian;

import gofabian.example.BookPojo;
import gofabian.example.BookRecord;
import gofabian.example.BookTable;
import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RecordTest {

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    void before() {
        Query query = dslContext.createTable(DSL.name("book"))
                .column(DSL.field(DSL.name("id"), Long.class), SQLDataType.BIGINT.identity(true))
                .column(DSL.field(DSL.name("name"), String.class), SQLDataType.VARCHAR)
                .column(DSL.field(DSL.name("timestamp"), LocalDateTime.class), SQLDataType.LOCALDATETIME.defaultValue(DSL.currentLocalDateTime()))
                .constraint(DSL.constraint("pk_id").primaryKey(DSL.name("id")));
        databaseClient.execute(query.getSQL()).fetch().rowsUpdated().block();
    }

    @AfterEach
    void after() {
        Query query = dslContext.dropTable(DSL.name("book"));
        databaseClient.execute(query.getSQL()).fetch().rowsUpdated().block();
    }

    @Test
    void insertRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).value1(42L).value2("Java Basics");
        Integer insertCount = ReactiveJooq.insert(record).block();
        assertEquals(1, insertCount);

        Record fetchedRecord = ReactiveJooq.fetchOne(dslContext.selectFrom(BookTable.BOOK_TABLE)).block();
        assertNotNull(fetchedRecord);
        assertEquals(record.into(BookPojo.class), fetchedRecord.into(BookPojo.class));
    }

    @Test
    void updateRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).value1(42L).value2("Java Basics");
        ReactiveJooq.insert(record).block();

        record.value2("C++ Basics");
        Integer updateCount = ReactiveJooq.update(record).block();
        assertEquals(1, updateCount);

        Record fetchedRecord = ReactiveJooq.fetchOne(dslContext.selectFrom(BookTable.BOOK_TABLE)).block();
        assertNotNull(fetchedRecord);
        BookPojo fetchedBook = fetchedRecord.into(BookPojo.class);
        assertEquals(42L, fetchedBook.getId());
        assertEquals("C++ Basics", fetchedBook.getName());
    }

    @Test
    void deleteRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).value1(1337L).value2("Olymp");
        ReactiveJooq.insert(record).block();

        Integer deleteCount = ReactiveJooq.delete(record).block();
        assertEquals(1, deleteCount);

        List<?> fetchedRecords = ReactiveJooq.fetch(dslContext.selectFrom(BookTable.BOOK_TABLE)).collectList().block();
        assertNotNull(fetchedRecords);
        assertEquals(0, fetchedRecords.size());
    }

    @Test
    void genericRecordResult() {
        {
            BookRecord preparedRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value1(1337L).value2("Olymp");
            ReactiveJooq.insert(preparedRecord).block();
        }
        {
            Select<? extends Record> select = dslContext.selectFrom(BookTable.BOOK_TABLE);
            Record record = ReactiveJooq.fetchOne(select).block();
            assertTrue(record instanceof BookRecord);
        }
        {
            Select<? extends Record> select = dslContext
                    .select(BookTable.BOOK_TABLE.ID, BookTable.BOOK_TABLE.NAME)
                    .from(BookTable.BOOK_TABLE);
            Record record = ReactiveJooq.fetchOne(select).block();
            assertTrue(record instanceof Record2);
        }
    }

    @Test
    void storeAsInsert() {
        // when: store new record
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value1(19L).value2("book name");
        {
            Integer insertResult = ReactiveJooq.store(bookRecord).block();
            assertNotNull(insertResult);
            assertEquals(1L, insertResult.intValue());
        }

        // then: inserted
        {
            Select<? extends Record> select = dslContext.selectFrom(BookTable.BOOK_TABLE);
            Record record = ReactiveJooq.fetchOne(select).block();
            assertNotNull(record);
            assertEquals(19L, record.get(0, Long.class));
            assertEquals("book name", record.get(1, String.class));
        }

        // when: store record with updated primary key
        {
            bookRecord.value1(20L);
            Integer insertResult = ReactiveJooq.store(bookRecord).block();
            assertNotNull(insertResult);
            assertEquals(1L, insertResult.intValue());
        }

        // then: inserted second record
        {
            Select<? extends Record> select = dslContext.selectFrom(BookTable.BOOK_TABLE);
            List<? extends Record> records = ReactiveJooq.fetch(select).collectList().block();
            assertNotNull(records);
            records.sort(Comparator.comparing(r -> r.get(0, Long.class)));
            assertEquals(19L, records.get(0).get(0, Long.class));
            assertEquals("book name", records.get(0).get(1, String.class));
            assertEquals(20L, records.get(1).get(0, Long.class));
            assertEquals("book name", records.get(1).get(1, String.class));
        }
    }

    @Test
    void storeAsUpdate() {
        // when: store new record
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value1(19L).value2("book name");
        ReactiveJooq.store(bookRecord).block();

        // when: store updated record
        bookRecord.value2("book name changed");
        Integer updateResult = ReactiveJooq.store(bookRecord).block();
        assertNotNull(updateResult);
        assertEquals(1L, updateResult.intValue());

        // then: updated
        Select<? extends Record> select = dslContext.selectFrom(BookTable.BOOK_TABLE);
        Record record = ReactiveJooq.fetchOne(select).block();
        assertNotNull(record);
        assertEquals(19L, record.get(0, Long.class));
        assertEquals("book name changed", record.get(1, String.class));
    }

    @Test
    void unchangedAfterExecution() {
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value1(19L).value2("book name");
        assertTrue(bookRecord.changed());
        ReactiveJooq.insert(bookRecord).block();
        assertFalse(bookRecord.changed());

        bookRecord.value2("another book name");
        assertTrue(bookRecord.changed());
        ReactiveJooq.update(bookRecord).block();
        assertFalse(bookRecord.changed());
    }

    @Test
    void changedAfterDeletion() {
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value1(19L).value2("book name");
        ReactiveJooq.insert(bookRecord).block();

        assertFalse(bookRecord.changed());
        ReactiveJooq.delete(bookRecord).block();
        assertTrue(bookRecord.changed());
    }

    @Test
    void refreshPrimaryKeyAfterInsert() {
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value2("book name");
        ReactiveJooq.insert(bookRecord).block();

        assertNotNull(bookRecord.value1()); // generated primary key returned by default
        assertNull(bookRecord.value3()); // generated but _not_ returned by default
    }

    @Test
    void refreshAllAfterInsert() {
        try {
            dslContext.settings().setReturnAllOnUpdatableRecord(true);
            BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value2("book name");
            ReactiveJooq.insert(bookRecord).block();

            assertNotNull(bookRecord.value1()); // generated primary key
            assertNotNull(bookRecord.value3()); // generated normal column
        } finally {
            dslContext.settings().setReturnAllOnUpdatableRecord(false);
        }
    }

    @Test
    void refreshAllAfterUpdate() {
        try {
            dslContext.settings().setReturnAllOnUpdatableRecord(true);

            // prepare row
            BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value2("book name");
            ReactiveJooq.insert(bookRecord).block();

            // change row outside of TableRecord
            LocalDateTime changedTimestamp = LocalDateTime.now().plusYears(10).withNano(0);
            Query updateQuery = dslContext.update(BookTable.BOOK_TABLE)
                    .set(BookTable.BOOK_TABLE.TIMESTAMP, changedTimestamp)
                    .where(BookTable.BOOK_TABLE.ID.eq(bookRecord.value1()));
            ReactiveJooq.execute(updateQuery).block();

            // change row via TableRecord
            bookRecord.value2("changed book name");
            Integer updateResult = ReactiveJooq.update(bookRecord).block();
            assertEquals(1, updateResult);

            // both changes should be in the result
            assertEquals("changed book name", bookRecord.value2());
            assertEquals(changedTimestamp, bookRecord.value3());
        } finally {
            dslContext.settings().setReturnAllOnUpdatableRecord(false);
        }
    }

    @Test
    void refresh() {
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).value2("book name");
        ReactiveJooq.insert(bookRecord).block();

        Query updateQuery = dslContext.update(BookTable.BOOK_TABLE).set(BookTable.BOOK_TABLE.NAME, "changed name");
        ReactiveJooq.execute(updateQuery).block();

        ReactiveJooq.refresh(bookRecord).block();

        assertEquals("changed name", bookRecord.value2());
    }

}
