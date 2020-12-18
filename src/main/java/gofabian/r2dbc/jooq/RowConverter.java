package gofabian.r2dbc.jooq;

import gofabian.r2dbc.jooq.converter.Converter;
import io.r2dbc.spi.Row;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;

import java.util.List;
import java.util.Objects;

class RowConverter {

    private final Converter converter;

    public RowConverter(Converter converter) {
        this.converter = Objects.requireNonNull(converter);
    }

    public <R extends Record> R convertRowToRecord(DSLContext dslContext, Row row, List<Field<?>> fields,
                                                   Class<? extends R> recordType) {
        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Object value = row.get(i, Object.class);
            Class<?> targetType = fields.get(i).getConverter().fromType();
            values[i] = converter.toJooqValue(value, targetType);
        }

        // create intermediate record
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        return record.into(recordType);
    }

}
