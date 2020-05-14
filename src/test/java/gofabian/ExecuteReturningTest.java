package gofabian;

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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ExecuteReturningTest {

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
    void executeInsertReturning() {
        InsertResultStep<BookRecord> query = dslContext
                .insertInto(BookTable.BOOK_TABLE, BookTable.BOOK_TABLE.NAME)
                .values("bob")
                .values("pia")
                .returning(BookTable.BOOK_TABLE.ID);
        List<BookRecord> records = ReactiveJooq.executeReturning(query).collectList().block();

        assertNotNull(records);
        if (dslContext.family() == SQLDialect.MYSQL) {
            // MySQL can only return one row with "last_insert_id"
            assertEquals(1, records.size());
            assertNotNull(records.get(0).get(BookTable.BOOK_TABLE.ID));
        } else {
            assertEquals(2, records.size());
            assertNotNull(records.get(0).get(BookTable.BOOK_TABLE.ID));
            assertNotNull(records.get(1).get(BookTable.BOOK_TABLE.ID));
        }
    }

    @Test
    void executeInsertReturningOne() {
        InsertResultStep<BookRecord> query = dslContext
                .insertInto(BookTable.BOOK_TABLE, BookTable.BOOK_TABLE.NAME)
                .values("tom")
                .returning(BookTable.BOOK_TABLE.ID);
        BookRecord record = ReactiveJooq.executeReturningOne(query).block();

        assertNotNull(record);
        assertNotNull(record.get(BookTable.BOOK_TABLE.ID));
    }

    @Test
    void executeUpdateReturning() {
        Query insertQuery = dslContext
                .insertInto(BookTable.BOOK_TABLE, BookTable.BOOK_TABLE.NAME)
                .values("aaa")
                .values("aaa");
        ReactiveJooq.execute(insertQuery).block();

        UpdateResultStep<BookRecord> query = dslContext
                .update(BookTable.BOOK_TABLE)
                .set(BookTable.BOOK_TABLE.NAME, "bob")
                .where(BookTable.BOOK_TABLE.NAME.eq("aaa"))
                .returning(BookTable.BOOK_TABLE.NAME);
        List<BookRecord> records = ReactiveJooq.executeReturning(query).collectList().block();

        assertNotNull(records);
        if (dslContext.family() == SQLDialect.MYSQL) {
            // MySQL does not support "UPDATE ... RETURNING"
            assertEquals(0, records.size());
        } else {
            assertEquals(2, records.size());
            assertEquals("bob", records.get(0).get(BookTable.BOOK_TABLE.NAME));
            assertEquals("bob", records.get(1).get(BookTable.BOOK_TABLE.NAME));
        }
    }

    @Test
    void executeUpdateReturningOne() {
        Query insertQuery = dslContext
                .insertInto(BookTable.BOOK_TABLE, BookTable.BOOK_TABLE.NAME)
                .values("tom");
        ReactiveJooq.execute(insertQuery).block();

        UpdateResultStep<BookRecord> query = dslContext
                .update(BookTable.BOOK_TABLE)
                .set(BookTable.BOOK_TABLE.NAME, "bob")
                .where(BookTable.BOOK_TABLE.NAME.eq("tom"))
                .returning(BookTable.BOOK_TABLE.NAME);
        BookRecord record = ReactiveJooq.executeReturningOne(query).block();

        if (dslContext.family() == SQLDialect.MYSQL) {
            // MySQL does not support "UPDATE ... RETURNING"
            assertNull(record);
        } else {
            assertNotNull(record);
            assertEquals("bob", record.get(BookTable.BOOK_TABLE.NAME));
        }
    }

}
