package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class PixelsConnectorId {
    private final String id;

    @JsonCreator
    public PixelsConnectorId(@JsonProperty("id") String id) {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PixelsConnectorId other = (PixelsConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}
