package gofabian.db;

import gofabian.ExecuteReturningTest;
import gofabian.QueryTest;
import gofabian.RecordTest;
import org.junit.jupiter.api.Nested;
import org.springframework.test.context.TestPropertySource;


public class PostgresqlTest {

    static final String R2DBC_URL_PROPERTY = "spring.r2dbc.url=r2dbc:tc:postgresql:///db?TC_IMAGE_TAG=9.6.8";

    @Nested
    @TestPropertySource(properties = PostgresqlTest.R2DBC_URL_PROPERTY)
    class PgQueryTest extends QueryTest {

    }

    @Nested
    @TestPropertySource(properties = PostgresqlTest.R2DBC_URL_PROPERTY)
    class PgRecordTest extends RecordTest {
    }

    @Nested
    @TestPropertySource(properties = PostgresqlTest.R2DBC_URL_PROPERTY)
    class PgExecuteReturningTest extends ExecuteReturningTest {
    }

}
