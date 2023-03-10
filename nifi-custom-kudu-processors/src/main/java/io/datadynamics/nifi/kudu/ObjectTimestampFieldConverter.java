package io.datadynamics.nifi.kudu;

import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

/**
 * Convert Object to java.sql.Timestamp using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
public class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {
    /**
     * Convert Object field to java.sql.Timestamp using optional format supported in DateTimeFormatter
     *
     * @param field   Field can be null or a supported input type
     * @param pattern Format pattern optional for parsing
     * @param name    Field name for tracking
     * @return Timestamp or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public Timestamp convertField(final Object field, final Optional<String> pattern, final String name) {
        if (field == null) {
            return null;
        }
        if (field instanceof Timestamp) {
            return (Timestamp) field;
        }
        if (field instanceof Date) {
            final Date date = (Date) field;
            return new Timestamp(date.getTime());
        }
        if (field instanceof Number) {
            final Number number = (Number) field;
            return new Timestamp(number.longValue());
        }
        if (field instanceof String) {
            final String string = field.toString().trim();
            if (string.isEmpty()) {
                return null;
            }

            if (pattern.isPresent()) {
                final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern.get());
                try {
                    final LocalDateTime localDateTime = LocalDateTime.parse(string, formatter);
                    return Timestamp.valueOf(localDateTime);
                } catch (final DateTimeParseException e) {
                    final String message = String.format("Convert Field Name [%s] Value [%s] to Timestamp LocalDateTime parsing failed: %s", name, field, e.getMessage());
                    throw new IllegalTypeConversionException(message);
                }
            } else {
                try {
                    final long number = Long.parseLong(string);
                    return new Timestamp(number);
                } catch (final NumberFormatException e) {
                    final String message = String.format("Convert Field Name [%s] Value [%s] to Timestamp Long parsing failed: %s", name, field, e.getMessage());
                    throw new IllegalTypeConversionException(message);
                }
            }
        }

        final String message = String.format("Convert Field Name [%s] Value [%s] Class [%s] to Timestamp not supported", name, field, field.getClass());
        throw new IllegalTypeConversionException(message);
    }
}