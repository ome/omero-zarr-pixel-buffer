package com.glencoesoftware.omero.zarr;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for safe, centralized handling of generic casts used by
 * OMERO Zarr classes.
 */
public abstract class Utils {

    /**
     * Casts an {@link Object} to {@code Map<String, Object>} with basic runtime
     * type checks.
     *
     * @param value the value to cast
     * @return the value cast to {@code Map<String, Object>}
     * @throws IllegalArgumentException if {@code value} is {@code null} or not a
     *                                  {@link Map}
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> castToStringObjectMap(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Expected non-null Map value");
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException(
                "Expected Map but was " + value.getClass().getName());
        }
        return (Map<String, Object>) value;
    }

    /**
     * Casts an {@link Object} to {@code List<Map<String, Object>>} with basic
     * runtime type checks.
     *
     * @param value the value to cast
     * @return the value cast to {@code List<Map<String, Object>>}
     * @throws IllegalArgumentException if {@code value} is {@code null}, not a
     *                                  {@link List}, or contains non-Map
     *                                  elements
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> castToListOfObjectMap(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Expected non-null List value");
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                "Expected List but was " + value.getClass().getName());
        }
        for (Object element : (List<?>) value) {
            if (!(element instanceof Map)) {
                throw new IllegalArgumentException("List must contain only Map elements");
            }
        }
        return (List<Map<String, Object>>) value;
    }

    /**
     * Casts an {@link Object} to {@code List<Map<String, String>>} with basic
     * runtime type checks.
     *
     * @param value the value to cast
     * @return the value cast to {@code List<Map<String, String>>}
     * @throws IllegalArgumentException if {@code value} is {@code null}, not a
     *                                  {@link List}, or contains non-Map
     *                                  elements
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, String>> castToListOfStringMap(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Expected non-null List value");
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                "Expected List but was " + value.getClass().getName());
        }
        for (Object element : (List<?>) value) {
            if (!(element instanceof Map)) {
                throw new IllegalArgumentException(
                    "List must contain only Map elements with String keys");
            }
        }
        return (List<Map<String, String>>) value;
    }

}
