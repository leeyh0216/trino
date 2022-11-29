/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import io.trino.metadata.CatalogManager;
import io.trino.metadata.InternalNodeManager;
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

@Path("/v1/catalog")
public class CatalogResource
{
    private final CatalogManager catalogManager;

    private final InternalNodeManager internalNodeManager;

    private final DynamicCatalogService dynamicCatalogService;

    @Inject
    public CatalogResource(CatalogManager catalogManager, InternalNodeManager internalNodeManager, DynamicCatalogService dynamicCatalogService)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.dynamicCatalogService = requireNonNull(dynamicCatalogService, "dynamicCatalogService is null");
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response createCatalog(CatalogCreateRequest request)
    {
        catalogManager.createCatalog(request.getCatalog(), request.getConnector(), request.getProperties());
        internalNodeManager.refreshNodes();
        dynamicCatalogService.refreshCatalogHandleIds();
        return Response.status(Response.Status.OK).build();
    }
}
