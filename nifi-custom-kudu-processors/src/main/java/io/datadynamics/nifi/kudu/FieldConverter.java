package io.datadynamics.nifi.kudu;
import java.util.Optional;

/**
 * Generalized Field Converter interface for handling type conversion with optional format parsing
 *
 * @param <I> Input Field Type
 * @param <O> Output Field Type
 */
public interface FieldConverter<I, O> {
    /**
     * Convert Field using Output Field Type with optional format parsing
     *
     * @param field Input field to be converted
     * @param pattern Format pattern optional for parsing
     * @param name Input field name for tracking
     * @return Converted Field can be null when input field is null or empty
     */
    O convertField(I field, Optional<String> pattern, String name);
}