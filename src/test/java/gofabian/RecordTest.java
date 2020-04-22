package gofabian;

import gofabian.db.BookPojo;
import gofabian.db.BookRecord;
import gofabian.db.BookTable;
import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        Integer insertCount = ReactiveJooq.executeInsert(record).block();
        assertEquals(1, insertCount);

        Record fetchedRecord = ReactiveJooq.fetchOne(dslContext.selectFrom(BookTable.BOOK_TABLE)).block();
        assertNotNull(fetchedRecord);
        assertEquals(record.into(BookPojo.class), fetchedRecord.into(BookPojo.class));
    }

    @Test
    void updateRecord() {
        BookRecord record = dslContext.newRecord(BookTable.BOOK_TABLE).values(42L, "Java Basics");
        ReactiveJooq.executeInsert(record).block();

        record.value2("C++ Basics");
        Integer updateCount = ReactiveJooq.executeUpdate(record).block();
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
        ReactiveJooq.executeInsert(record).block();

        Integer deleteCount = ReactiveJooq.executeDelete(record).block();
        assertEquals(1, deleteCount);

        List<Record> fetchedRecords = ReactiveJooq.fetch(dslContext.selectFrom(BookTable.BOOK_TABLE)).collectList().block();
        assertNotNull(fetchedRecords);
        assertEquals(0, fetchedRecords.size());
    }

}
