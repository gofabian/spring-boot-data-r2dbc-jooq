package gofabian.r2dbc.jooq.converter;

import io.r2dbc.postgresql.codec.Json;
import org.jooq.JSON;

public class PostgresJsonConverter extends AbstractConverter<JSON, Json> {

    public PostgresJsonConverter() {
        super(JSON.class, Json.class);
    }

    @Override
    protected JSON toTypedJooqValue(Json r2dbcValue) {
        return JSON.valueOf(r2dbcValue.asString());
    }

    @Override
    protected Json toTypedR2dbcValue(JSON jooqValue) {
        return Json.of(jooqValue.data());
    }

}
