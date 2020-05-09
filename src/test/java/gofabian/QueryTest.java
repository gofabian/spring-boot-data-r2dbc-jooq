package gofabian;

import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.util.List;

import static org.jooq.impl.DSL.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
//@SpringBootTest(properties = "spring.r2dbc.url=r2dbc:tc:postgresql:///db?TC_IMAGE_TAG=9.6.8")
//@SpringBootTest(properties = "spring.r2dbc.url=r2dbc:tc:mysql:///db?TC_IMAGE_TAG=5.6.23")
class QueryTest {

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    void before() {
        {
            Query query = dslContext.createTable(name("tab"))
                    .column(field(name("id"), Long.class), SQLDataType.BIGINT.identity(true))
                    .column(field(name("name"), String.class), SQLDataType.VARCHAR)
                    .constraint(constraint(name("pk_id2")).primaryKey(name("id")));
            databaseClient.execute(query.getSQL()).fetch().rowsUpdated().block();
        }
        {
            Query query = dslContext.insertInto(table(name("tab")),
                    field(name("name"), String.class))
                    .values("fab");
            databaseClient.execute(query.getSQL(ParamType.INLINED)).fetch().rowsUpdated().block();
        }
    }

    @AfterEach
    void after() {
        Query query = dslContext.dropTable(name("tab"));
        databaseClient.execute(query.getSQL()).fetch().rowsUpdated().block();
    }

    @Test
    void execute() {
        Query query = dslContext
                .insertInto(table(name("tab")))
                .set(field(name("name")), "another fab");
        Integer insertCount = ReactiveJooq.execute(query).block();
        assertEquals(1, insertCount);
    }

    @Test
    void executeInsertReturning() {
        InsertResultStep<Record> query = dslContext
                .insertInto(table(name("tab")), field(name("name"), String.class))
                .values("bob")
                .values("pia")
                .returning(field(name("id"), Long.class));
        List<Record> records = ReactiveJooq.executeReturning(query).collectList().block();

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(1, records.get(0).size());
        assertEquals("id", records.get(0).field(0).getName());
        assertTrue(records.get(0).get(0) instanceof Long);
        assertEquals(1, records.get(1).size());
        assertEquals("id", records.get(1).field(0).getName());
        assertTrue(records.get(1).get(0) instanceof Long);
    }

    @Test
    void executeInsertReturningOne() {
        InsertResultStep<Record> query = dslContext
                .insertInto(table(name("tab")), field(name("name"), String.class))
                .values("bob")
                .returning(field(name("id"), Long.class));
        Record record = ReactiveJooq.executeReturningOne(query).block();

        assertNotNull(record);
        assertEquals(1, record.size());
        assertEquals("id", record.field(0).getName());
        assertTrue(record.get(0) instanceof Long);
    }

    @Test
    void executeUpdateReturning() {
        Query insertQuery = dslContext
                .insertInto(table(name("tab")), field(name("name"), String.class))
                .values("aaa")
                .values("aaa");
        ReactiveJooq.execute(insertQuery).block();

        UpdateResultStep<Record> query = dslContext
                .update(table(name("tab")))
                .set(field(name("name"), String.class), "bob")
                .where(field(name("name"), String.class).eq("aaa"))
                .returning(field(name("name"), String.class));
        List<Record> records = ReactiveJooq.executeReturning(query).collectList().block();

        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(1, records.get(0).size());
        assertEquals("name", records.get(0).field(0).getName());
        assertEquals("bob", records.get(0).get(0));
        assertEquals(1, records.get(1).size());
        assertEquals("name", records.get(1).field(0).getName());
        assertEquals("bob", records.get(1).get(0));
    }

    @Test
    void executeUpdateReturningOne() {
        UpdateResultStep<Record> query = dslContext
                .update(table(name("tab")))
                .set(field(name("name"), String.class), "bob")
                .where(field(name("name"), String.class).eq("fab"))
                .returning(field(name("name"), String.class));
        Record record = ReactiveJooq.executeReturningOne(query).block();

        assertNotNull(record);
        assertEquals(1, record.size());
        assertEquals("name", record.field(0).getName());
        assertEquals("bob", record.get(0));
    }

    @Test
    void fetch() {
        Select<?> query = dslContext
                .select(field(name("id"), Long.class), field(name("name"), String.class))
                .from(name("tab"));
        List<? extends Record> records = ReactiveJooq.fetch(query).collectList().block();
        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals("fab", records.get(0).get(name("name"), String.class));
    }

    @Test
    void fetchOne() {
        Select<?> query = dslContext
                .select(field(name("id"), Long.class), field(name("name"), String.class))
                .from(name("tab"));
        Record record = ReactiveJooq.fetchOne(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get(name("name"), String.class));
    }

    @Test
    void fetchAny() {
        Select<?> query = dslContext
                .select(field(name("id"), Long.class), field(name("name"), String.class))
                .from(name("tab"));
        Record record = ReactiveJooq.fetchAny(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get(name("name"), String.class));
    }

    @Test
    void fetchExists() {
        Select<?> query = dslContext
                .select(field(name("id"), Long.class), field(name("name"), String.class))
                .from(name("tab"));
        Boolean exists = ReactiveJooq.fetchExists(query).block();
        assertNotNull(exists);
        assertTrue(exists);
    }

    @Test
    void fetchCount() {
        Select<?> query = dslContext
                .select(field(name("id"), Long.class), field(name("name"), String.class))
                .from(name("tab"));
        Integer count = ReactiveJooq.fetchCount(query).block();
        assertEquals(1, count);
    }

}
