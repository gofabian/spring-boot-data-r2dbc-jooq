package gofabian;

import gofabian.db.MySqlTest;
import gofabian.example.JsonRecord;
import gofabian.r2dbc.jooq.ReactiveJooq;
import org.jooq.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.TestPropertySource;

import static gofabian.example.JsonTable.JSON_TABLE;
import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = MySqlTest.R2DBC_URL_PROPERTY)
public class MysqlJsonTest {

    private static final JSON JSON_VALUE = JSON.valueOf("{\"key\": \"value\"}");
    private static final JSONB JSONB_VALUE = JSONB.valueOf("{\"key\": \"value\"}");

    @Autowired
    DatabaseClient databaseClient;
    @Autowired
    DSLContext dslContext;

    @BeforeEach
    public void beforeEach() {
        {
            Query query = dslContext.createTable(JSON_TABLE)
                    .column(JSON_TABLE.ID)
                    .column(JSON_TABLE.JSON)
                    .column(JSON_TABLE.JSONB)
                    .constraint(constraint(name("pk_id3")).primaryKey(JSON_TABLE.ID));
            databaseClient.sql(query.getSQL()).fetch().rowsUpdated().block();
        }
    }

    @AfterEach
    public void afterEach() {
        Query query = dslContext.dropTable(JSON_TABLE);
        databaseClient.sql(query.getSQL()).fetch().rowsUpdated().block();
    }

    @Test
    public void convertJson() {
        Long id;
        {
            InsertResultStep<JsonRecord> insert = dslContext
                    .insertInto(JSON_TABLE, JSON_TABLE.JSON)
                    .values((JSON) null)
                    .returning();
            JsonRecord record = ReactiveJooq.executeReturningOne(insert).block();

            assertNotNull(record);
            assertNotNull(record.value1());
            id = record.value1();
        }
        {
            Select<Record2<Long, JSON>> select = dslContext.select(JSON_TABLE.ID, JSON_TABLE.JSON)
                    .from(JSON_TABLE)
                    .where(JSON_TABLE.ID.eq(id));
            Record2<Long, JSON> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertNull(record.value2());
        }
        {
            InsertResultStep<JsonRecord> insert = dslContext
                    .insertInto(JSON_TABLE, JSON_TABLE.JSON)
                    .values(JSON_VALUE)
                    .returning();
            JsonRecord record = ReactiveJooq.executeReturningOne(insert).block();

            assertNotNull(record);
            assertNotNull(record.value1());
            id = record.value1();
        }
        {
            Select<Record2<Long, JSON>> select = dslContext.select(JSON_TABLE.ID, JSON_TABLE.JSON)
                    .from(JSON_TABLE)
                    .where(JSON_TABLE.ID.eq(id));
            Record2<Long, JSON> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertEquals(JSON_VALUE, record.value2());
        }
    }


    @Test
    public void convertJsonb() {
        Long id;
        {
            InsertResultStep<JsonRecord> insert = dslContext
                    .insertInto(JSON_TABLE, JSON_TABLE.JSONB)
                    .values((JSONB) null)
                    .returning();
            JsonRecord record = ReactiveJooq.executeReturningOne(insert).block();

            assertNotNull(record);
            assertNotNull(record.value1());
            id = record.value1();
        }
        {
            Select<Record2<Long, JSONB>> select = dslContext.select(JSON_TABLE.ID, JSON_TABLE.JSONB)
                    .from(JSON_TABLE)
                    .where(JSON_TABLE.ID.eq(id));
            Record2<Long, JSONB> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertNull(record.value2());
        }
        {
            InsertResultStep<JsonRecord> insert = dslContext
                    .insertInto(JSON_TABLE, JSON_TABLE.JSONB)
                    .values(JSONB_VALUE)
                    .returning();
            JsonRecord record = ReactiveJooq.executeReturningOne(insert).block();

            assertNotNull(record);
            assertNotNull(record.value1());
            id = record.value1();
        }
        {
            Select<Record2<Long, JSONB>> select = dslContext.select(JSON_TABLE.ID, JSON_TABLE.JSONB)
                    .from(JSON_TABLE)
                    .where(JSON_TABLE.ID.eq(id));
            Record2<Long, JSONB> record = ReactiveJooq.fetchOne(select).block();

            assertNotNull(record);
            assertEquals(JSONB_VALUE, record.value2());
        }
    }

}
