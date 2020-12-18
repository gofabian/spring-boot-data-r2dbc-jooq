package gofabian.r2dbc.jooq.converter;

import io.r2dbc.postgresql.codec.Json;

public class PostgresJsonConverter implements Converter {

    @Override
    public Object toJooqValue(Object r2dbcValue, Class<?> targetJooqType) {
        if (r2dbcValue instanceof Json) {
            return ((Json) r2dbcValue).asString();
        }
        return r2dbcValue;
    }

    @Override
    public Object toR2dbcValue(Object jooqValue) {
        return jooqValue;
    }

    @Override
    final public Class<?> toR2dbcType(Class<?> sourceJooqType) {
        return sourceJooqType;
    }

}
