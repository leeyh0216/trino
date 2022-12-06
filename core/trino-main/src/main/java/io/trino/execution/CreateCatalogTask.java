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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.DynamicCatalogService;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class CreateCatalogTask
        implements DataDefinitionTask<CreateCatalog>
{
    private final CatalogManager catalogManager;
    private final InternalNodeManager internalNodeManager;
    private final DynamicCatalogService dynamicCatalogService;

    @Inject
    public CreateCatalogTask(CatalogManager catalogManager, InternalNodeManager internalNodeManager, DynamicCatalogService dynamicCatalogService)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.dynamicCatalogService = requireNonNull(dynamicCatalogService, "dynamicCatalogService is null");
    }

    @Override
    public String getName()
    {
        return "CREATE CATALOG";
    }

    @Override
    public ListenableFuture<Void> execute(CreateCatalog statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Map<String, String> properties = new HashMap<>();
        statement.getProperties().stream().forEach(p -> properties.put(p.getName(), p.getValue().toString().replaceAll("'", "")));
        catalogManager.createCatalog(statement.getCatalogName().toString(), statement.getPluginName(), properties);
        internalNodeManager.refreshNodes();
        dynamicCatalogService.refreshCatalogHandleIds();
        return immediateVoidFuture();
    }
}
