package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.tools.JooqLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RowConverter {

    private static final JooqLogger log = JooqLogger.getLogger(RowConverter.class);
    
    private static final Map<Class<?>, Class<?>> forcedTypes = new HashMap<>();

    static <R extends Record> R convertRowToRecord(DSLContext dslContext, Row row, List<Field<?>> fields,
                                                   Class<? extends R> recordType) {
        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field<?> field = fields.get(i);
            try {
                values[i] = row.get(i, getType(field));
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

    private static Class<?> getType(final Field<?> field) {
		final Class<?> type = field.getType();
		return forcedTypes.getOrDefault(type, type);
	}

	/**
	 * Forces a data type to be consumed from the database as another type. <br>
	 * <br>
	 * 
	 * For example JOOQ currently does not have any mappings from r2dbc-postgresql's
	 * io.r2dbc.postgresql.codec.Json to a {@link Record}'s expected {@link JSONB}.
	 * However, r2dbc-postgresql can interpret a JSON/JSONB column as a String and
	 * then JSONB can read the String. Hence, you can override how the data will be
	 * interpreted from the database based on the Record's expected type.
	 * 
	 * @param from the expected type in JOOQ record
	 * @param to   the nearest compatible type for both the column type and the
	 *             Record's field
	 */
	public static void addForcedType(Class<?> from, Class<?> to) {
		forcedTypes.put(from, to);
	}

	/**
	 * Returns all of the custom mappings that is applied to all records
	 * 
	 * @return all of the custom mappings
	 */
	public static Map<Class<?>, Class<?>> getAllForcedTypes() {
		return Collections.unmodifiableMap(forcedTypes);
	}
}
