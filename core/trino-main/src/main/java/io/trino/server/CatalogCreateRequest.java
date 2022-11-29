package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CatalogCreateRequest {
    private final String catalog;

    private final String connector;

    private final Map<String, String> properties;

    @JsonCreator
    public CatalogCreateRequest(@JsonProperty("catalog") String catalog, @JsonProperty("connector") String connector,
                                @JsonProperty("properties") Map<String, String> properties) {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    @JsonProperty
    public String getCatalog() {
        return catalog;
    }

    @JsonProperty
    public String getConnector() {
        return connector;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }
}
