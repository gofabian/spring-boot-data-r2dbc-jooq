package gofabian.r2dbc.jooq.converter;

public interface Converter {

    Object toJooqValue(Object r2dbcValue, Class<?> targetJooqType);

    Object toR2dbcValue(Object jooqValue);

    Class<?> toR2dbcType(Class<?> jooqType);

}
