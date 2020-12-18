package gofabian;

import gofabian.db.PostgresqlTest;
import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.*;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.TestPropertySource;

import static org.jooq.impl.DSL.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = PostgresqlTest.R2DBC_URL_PROPERTY)
public class PostgresqlJsonTest {

    private static final Table<Record> TABLE = table(name("tab"));
    private static final Field<Long> ID_FIELD = field(name("id"), SQLDataType.BIGINT.identity(true));
    private static final Field<JSON> JSON_FIELD = field(name("json"), SQLDataType.JSON);
    private static final Field<JSONB> JSONB_FIELD = field(name("jsonb"), SQLDataType.JSONB);
    private static final JSON JSON_VALUE = JSON.valueOf("{\"key\": \"value\"}");
    private static final JSONB JSONB_VALUE = JSONB.valueOf("{\"key\": \"value\"}");

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    public void beforeEach() {
        {
            Query query = dslContext.createTable(TABLE)
                    .column(ID_FIELD)
                    .column(JSON_FIELD)
                    .column(JSONB_FIELD)
                    .constraint(constraint(name("pk_id3")).primaryKey(ID_FIELD));
            databaseClient.sql(query.getSQL()).fetch().rowsUpdated().block();
        }
    }

    @AfterEach
    public void afterEach() {
        Query query = dslContext.dropTable(TABLE);
        databaseClient.sql(query.getSQL()).fetch().rowsUpdated().block();
    }

    @Test
    public void adaptPostgresJson() {
        {
            Query insert = dslContext
                    .insertInto(TABLE, ID_FIELD, JSON_FIELD)
                    .values(0L, null);
            Integer count = ReactiveJooq.execute(insert).block();
            assertEquals(1, count);
        }
        {
            Select<Record2<Long, JSON>> select = dslContext.select(ID_FIELD, JSON_FIELD)
                    .from(TABLE)
                    .where(ID_FIELD.eq(0L));
            Record2<Long, JSON> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertNull(record.value2());
        }
        {
            Query insert = dslContext
                    .insertInto(TABLE, ID_FIELD, JSON_FIELD)
                    .values(42L, JSON_VALUE);

            Integer count = ReactiveJooq.execute(insert).block();
            assertEquals(1, count);
        }
        {
            Select<Record2<Long, JSON>> select = dslContext.select(ID_FIELD, JSON_FIELD)
                    .from(TABLE)
                    .where(ID_FIELD.eq(42L));
            Record2<Long, JSON> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertEquals(JSON_VALUE, record.value2());
        }
    }

    @Test
    public void adaptPostgresJsonb() {
        {
            Query insert = dslContext
                    .insertInto(TABLE, ID_FIELD, JSONB_FIELD)
                    .values(0L, null);
            Integer count = ReactiveJooq.execute(insert).block();
            assertEquals(1, count);
        }
        {
            Select<Record2<Long, JSONB>> select = dslContext.select(ID_FIELD, JSONB_FIELD)
                    .from(TABLE)
                    .where(ID_FIELD.eq(0L));
            Record2<Long, JSONB> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertNull(record.value2());
        }
        {
            Query insert = dslContext
                    .insertInto(TABLE, ID_FIELD, JSONB_FIELD)
                    .values(42L, JSONB_VALUE);

            Integer count = ReactiveJooq.execute(insert).block();
            assertEquals(1, count);
        }
        {
            Select<Record2<Long, JSONB>> select = dslContext.select(ID_FIELD, JSONB_FIELD)
                    .from(TABLE)
                    .where(ID_FIELD.eq(42L));
            Record2<Long, JSONB> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertEquals(JSONB_VALUE, record.value2());
        }
    }

}
