package io.datadynamics.nifi.processor.processors.custom;

public enum OperationType {
    INSERT,
    INSERT_IGNORE,
    UPSERT,
    UPDATE,
    DELETE,
    UPDATE_IGNORE,
    DELETE_IGNORE;
}