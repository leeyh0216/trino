package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RefreshCatalog;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class RefreshCatalogTask
        implements DataDefinitionTask<RefreshCatalog>
{

    private final CatalogManager catalogManager;

    @Inject
    public RefreshCatalogTask(CatalogManager catalogManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @Override
    public String getName()
    {
        return "REFRESH CATALOG";
    }

    @Override
    public ListenableFuture<Void> execute(RefreshCatalog statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        catalogManager.refreshCatalog(statement.getCatalogName().toString());
        return immediateVoidFuture();
    }
}
