package gofabian;

import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.conf.ParamType;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.util.List;
import java.util.UUID;

import static org.jooq.impl.DSL.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class QueryTest {

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    void before() {
        {
            Query query = dslContext.createTable(name("tab"))
                    .column(field(name("id"), UUID.class), SQLDataType.UUID)
                    .column(field(name("name"), String.class), SQLDataType.VARCHAR)
                    .constraint(constraint(name("pk_id2")).primaryKey(name("id")));
            databaseClient.execute(query.getSQL()).fetch().rowsUpdated().block();
        }
        {
            Query query = dslContext.insertInto(table(name("tab")),
                    field(name("id"), UUID.class),
                    field(name("name"), String.class))
                    .values(UUID.randomUUID(), "fab");
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
                .set(field(name("id")), UUID.randomUUID())
                .set(field(name("name")), "another fab");
        Integer insertCount = ReactiveJooq.execute(query).block();
        assertEquals(1, insertCount);
    }

    @Test
    void fetch() {
        Select<?> query = dslContext
                .select(field(name("id"), UUID.class), field(name("name"), String.class))
                .from(name("tab"));
        List<? extends Record> records = ReactiveJooq.fetch(query).collectList().block();
        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals("fab", records.get(0).get(name("name"), String.class));
    }

    @Test
    void fetchOne() {
        Select<?> query = dslContext
                .select(field(name("id"), UUID.class), field(name("name"), String.class))
                .from(name("tab"));
        Record record = ReactiveJooq.fetchOne(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get(name("name"), String.class));
    }

    @Test
    void fetchAny() {
        Select<?> query = dslContext
                .select(field(name("id"), UUID.class), field(name("name"), String.class))
                .from(name("tab"));
        Record record = ReactiveJooq.fetchAny(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get(name("name"), String.class));
    }

    @Test
    void fetchExists() {
        Select<?> query = dslContext
                .select(field(name("id"), UUID.class), field(name("name"), String.class))
                .from(name("tab"));
        Boolean exists = ReactiveJooq.fetchExists(query).block();
        assertNotNull(exists);
        assertTrue(exists);
    }

    @Test
    void fetchCount() {
        Select<?> query = dslContext
                .select(field(name("id"), UUID.class), field(name("name"), String.class))
                .from(name("tab"));
        Integer count = ReactiveJooq.fetchCount(query).block();
        assertEquals(1, count);
    }

}
