package io.trino.server;

import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/plugin")
public class PluginResource
{
    private final PluginManager pluginManager;

    @Inject
    public PluginResource(PluginManager pluginManager)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response loadPlugin(PluginAddRequest request)
    {
        pluginManager.loadPluginDynamically(new DynamicPluginProvider(request.getName(), request.getDir()));
        return Response.status(Response.Status.OK).build();
    }
}
