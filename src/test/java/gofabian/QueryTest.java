package gofabian;

import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Select;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.DatabaseClient;

import java.util.List;
import java.util.UUID;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class QueryTest {

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    void before() {
        databaseClient.execute("create table tab ( id uuid primary key, name text );")
                .fetch().rowsUpdated().block();
        databaseClient.execute("insert into tab (id, name) values('" + UUID.randomUUID() + "', 'fab');").
                fetch().rowsUpdated().block();
    }

    @AfterEach
    void after() {
        databaseClient.execute("drop table tab;").fetch().rowsUpdated().block();
    }

    @Test
    void execute() {
        try {
            Query query = dslContext
                    .insertInto(table("tab"))
                    .set(field("id"), UUID.randomUUID())
                    .set(field("name"), "another fab");
            Integer insertCount = ReactiveJooq.execute(query).block();
            assertEquals(1, insertCount);
        } finally {
            databaseClient.execute("delete from tab where name = 'another fab';")
                    .fetch().rowsUpdated().block();
        }
    }

    @Test
    void fetch() {
        Select<?> query = dslContext
                .select(field("id"), field("name"))
                .from(table("tab"));
        List<? extends Record> records = ReactiveJooq.fetch(query).collectList().block();
        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals("fab", records.get(0).get("name", String.class));
    }

    @Test
    void fetchOne() {
        Select<?> query = dslContext
                .select(field("id"), field("name"))
                .from(table("tab"));
        Record record = ReactiveJooq.fetchOne(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get("name", String.class));
    }

    @Test
    void fetchAny() {
        Select<?> query = dslContext
                .select(field("id"), field("name"))
                .from(table("tab"));
        Record record = ReactiveJooq.fetchAny(query).block();
        assertNotNull(record);
        assertEquals("fab", record.get("name", String.class));
    }

    @Test
    void fetchExists() {
        Select<?> query = dslContext
                .select(field("id"), field("name"))
                .from(table("tab"));
        Boolean exists = ReactiveJooq.fetchExists(query).block();
        assertNotNull(exists);
        assertTrue(exists);
    }

    @Test
    void fetchCount() {
        Select<?> query = dslContext
                .select(field("id"), field("name"))
                .from(table("tab"));
        Integer count = ReactiveJooq.fetchCount(query).block();
        assertEquals(1, count);
    }

}
