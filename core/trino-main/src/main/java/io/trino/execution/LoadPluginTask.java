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
import io.trino.server.DynamicPluginProvider;
import io.trino.server.PluginManager;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LoadPlugin;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class LoadPluginTask
        implements DataDefinitionTask<LoadPlugin>
{
    private final PluginManager pluginManager;

    @Inject
    public LoadPluginTask(PluginManager pluginManager)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
    }

    @Override
    public String getName()
    {
        return "LOAD PLUGIN";
    }

    @Override
    public ListenableFuture<Void> execute(LoadPlugin statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        pluginManager.loadPluginDynamically(new DynamicPluginProvider(statement.getPluginName().toString(), statement.getDir()));
        return immediateVoidFuture();
    }
}
