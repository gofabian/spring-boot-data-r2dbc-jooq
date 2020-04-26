package gofabian;

import gofabian.db.BookPojo;
import gofabian.db.BookRecord;
import gofabian.db.BookTable;
import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Select;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class RecordTest {

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    void before() {
        databaseClient.execute("create table \"book\" ( \"id\" bigint primary key, \"name\" text );")
                .fetch().rowsUpdated().block();
    }

    @AfterEach
    void after() {
        databaseClient.execute("drop table \"book\";").
                fetch().rowsUpdated().block();
    }

    @Test
    void insertRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).values(42L, "Java Basics");
        Integer insertCount = ReactiveJooq.insert(record).block();
        assertEquals(1, insertCount);

        Record fetchedRecord = ReactiveJooq.fetchOne(dslContext.selectFrom(BookTable.BOOK_TABLE)).block();
        assertNotNull(fetchedRecord);
        assertEquals(record.into(BookPojo.class), fetchedRecord.into(BookPojo.class));
    }

    @Test
    void updateRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).values(42L, "Java Basics");
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
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).values(1337L, "Olymp");
        ReactiveJooq.insert(record).block();

        Integer deleteCount = ReactiveJooq.executeDelete(record).block();
        assertEquals(1, deleteCount);

        List<?> fetchedRecords = ReactiveJooq.fetch(dslContext.selectFrom(BookTable.BOOK_TABLE)).collectList().block();
        assertNotNull(fetchedRecords);
        assertEquals(0, fetchedRecords.size());
    }

    @Test
    void genericRecordResult() {
        {
            BookRecord preparedRecord = dslContext.newRecord(BookTable.BOOK_TABLE).values(1337L, "Olymp");
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
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).values(19L, "book name");
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
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).values(19L, "book name");
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
        BookRecord bookRecord = dslContext.newRecord(BookTable.BOOK_TABLE).values(19L, "book name");
        assertTrue(bookRecord.changed());
        ReactiveJooq.insert(bookRecord).block();
        assertFalse(bookRecord.changed());

        bookRecord.value2("another book name");
        assertTrue(bookRecord.changed());
        ReactiveJooq.update(bookRecord).block();
        assertFalse(bookRecord.changed());
    }

}
