package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PluginAddRequest
{
    private final String name;

    private final String dir;

    @JsonCreator
    public PluginAddRequest(@JsonProperty("name") String name, @JsonProperty("dir") String dir) {
        this.name = requireNonNull(name, "name is null");
        this.dir = requireNonNull(dir, "dir is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getDir()
    {
        return dir;
    }
}
