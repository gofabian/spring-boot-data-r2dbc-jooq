package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.tools.JooqLogger;

import java.util.List;

class RowConverter {

    private static final JooqLogger log = JooqLogger.getLogger(RowConverter.class);

    static <R extends Record> R convertRowToRecord(DSLContext dslContext, Row row, List<Field<?>> fields,
                                                   Class<? extends R> recordType) {
        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field<?> field = fields.get(i);
            try {
                values[i] = row.get(i, field.getType());
            } catch (IllegalArgumentException e) {
                // fallback: JOOQ RecordMapper converts the value later
                values[i] = row.get(i, Object.class);
                log.debug("R2DBC cannot convert value to field type: " + field.getType() + ", value=" + values[i]
                        + ", value type=" + (values[i] == null ? null : values[i].getClass()));
            }
        }

        // create intermediate record
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        return record.into(recordType);
    }

}
