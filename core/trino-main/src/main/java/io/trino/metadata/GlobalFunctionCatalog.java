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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.type.TypeSignature;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Locale.ENGLISH;

@ThreadSafe
public class GlobalFunctionCatalog
        implements FunctionProvider
{
    public static final String BUILTIN_SCHEMA = "builtin";
    private final Function<FunctionMapGenArgs, AbstractFunctionMap> functionMapGenFunction;
    private volatile AbstractFunctionMap functions;
    private volatile FunctionEvictEventListener functionEvictEventListener = (fid) -> {};

    public GlobalFunctionCatalog()
    {
        this(false);
    }

    @Inject
    public GlobalFunctionCatalog(FunctionConfig config)
    {
        //TODO GlobalFunctionCatalog를 Interface로 만들어야 할까? 그러면 다형성 측면에서는 도움이 되지만, Trino Upstream과 Merge 시 문제가 많이 발생할 수 있다.
        this(config.getDynamicFunctionLoading());
    }

    private GlobalFunctionCatalog(boolean dynamicFunctionLoading)
    {
        functionMapGenFunction = AbstractFunctionMap.functionMapGenFunction.apply(dynamicFunctionLoading);
        Supplier<AbstractFunctionMap> functionMapSupplier = AbstractFunctionMap.defaultFunctionMapGenFunction.apply(dynamicFunctionLoading);

        functions = functionMapSupplier.get();
    }

    public void setFunctionEvictEventListener(FunctionEvictEventListener functionEvictEventListener)
    {
        this.functionEvictEventListener = functionEvictEventListener;
    }

    public final synchronized void addFunctions(FunctionBundle functionBundle)
    {
        for (FunctionMetadata functionMetadata : functionBundle.getFunctions()) {
            checkArgument(!functionMetadata.getSignature().getName().contains("|"), "Function name cannot contain '|' character: %s", functionMetadata.getSignature());
            checkArgument(!functionMetadata.getSignature().getName().contains("@"), "Function name cannot contain '@' character: %s", functionMetadata.getSignature());
            checkNotSpecializedTypeOperator(functionMetadata.getSignature());
            this.functions.checkFunctionExists(functionMetadata);
        }
        this.functions = functionMapGenFunction.apply(new FunctionMapGenArgs(this.functions, functionBundle));

        //Function Cache Eviction. Load 이후에는 Invalid Cache 참조가 일어나지 않도록 한다.
        functionBundle.getFunctions().forEach(f -> functionEvictEventListener.evict(f.getFunctionId()));
    }

    /**
     * Type operators are handled automatically by the engine, so custom operator implementations
     * cannot be registered for these.
     */
    private static void checkNotSpecializedTypeOperator(Signature signature)
    {
        String name = signature.getName();
        if (!isOperatorName(name)) {
            return;
        }

        OperatorType operatorType = unmangleOperator(name);

        // The trick here is the Generic*Operator implementations implement these exact signatures,
        // so we only these exact signatures to be registered.  Since, only a single function with
        // a specific signature can be registered, it prevents others from being registered.
        Signature.Builder expectedSignature = Signature.builder()
                .name(signature.getName())
                .argumentTypes(Collections.nCopies(operatorType.getArgumentCount(), new TypeSignature("T")));

        switch (operatorType) {
            case EQUAL:
            case IS_DISTINCT_FROM:
            case INDETERMINATE:
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.comparableTypeParameter("T");
                break;
            case HASH_CODE:
            case XX_HASH_64:
                expectedSignature.returnType(BIGINT);
                expectedSignature.comparableTypeParameter("T");
                break;
            case COMPARISON_UNORDERED_FIRST:
            case COMPARISON_UNORDERED_LAST:
                expectedSignature.returnType(INTEGER);
                expectedSignature.orderableTypeParameter("T");
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.orderableTypeParameter("T");
                break;
            default:
                return;
        }

        checkArgument(signature.equals(expectedSignature.build()), "Can not register %s functionMetadata: %s", operatorType, signature);
    }

    public List<FunctionMetadata> listFunctions()
    {
        return functions.list();
    }

    public Collection<FunctionMetadata> getFunctions(SchemaFunctionName name)
    {
        if (!BUILTIN_SCHEMA.equals(name.getSchemaName())) {
            return ImmutableList.of();
        }
        return functions.get(name.getFunctionName());
    }

    public FunctionMetadata getFunctionMetadata(FunctionId functionId)
    {
        return functions.get(functionId);
    }

    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        return functions.getFunctionBundle(functionId).getAggregationFunctionMetadata(functionId);
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getWindowFunctionSupplier(functionId, boundSignature, functionDependencies);
    }

    @Override
    public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getAggregationImplementation(functionId, boundSignature, functionDependencies);
    }

    public FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        return functions.getFunctionBundle(functionId).getFunctionDependencies(functionId, boundSignature);
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        return functions.getFunctionBundle(functionId).getScalarFunctionImplementation(functionId, boundSignature, functionDependencies, invocationConvention);
    }

    private static class FunctionMapGenArgs
    {
        private final AbstractFunctionMap functionMap;
        private final FunctionBundle functionBundle;

        public FunctionMapGenArgs(AbstractFunctionMap functionMap, FunctionBundle functionBundle)
        {
            this.functionMap = functionMap;
            this.functionBundle = functionBundle;
        }

        public AbstractFunctionMap getFunctionMap()
        {
            return functionMap;
        }

        public FunctionBundle getFunctionBundle()
        {
            return functionBundle;
        }
    }

    private interface FunctionMap
    {
        List<FunctionMetadata> list();

        Collection<FunctionMetadata> get(String functionName);

        FunctionMetadata get(FunctionId functionId);

        FunctionBundle getFunctionBundle(FunctionId functionId);

        default void checkFunctionExists(FunctionMetadata functionMetadata)
        {
            //Nothing to do
        }
    }

    private abstract static class AbstractFunctionMap
            implements FunctionMap
    {
        public static Function<Boolean, Function<FunctionMapGenArgs, AbstractFunctionMap>> functionMapGenFunction = (dynamicLoading) ->
        {
            if (dynamicLoading) {
                return (genArg) -> new DynamicFunctionMap(genArg.getFunctionMap(), genArg.getFunctionBundle());
            }
            else {
                return (genArg) -> new StaticFunctionMap(genArg.getFunctionMap(), genArg.getFunctionBundle());
            }
        };

        public static Function<Boolean, Supplier<AbstractFunctionMap>> defaultFunctionMapGenFunction = (dynamicLoading) ->
        {
            if (dynamicLoading) {
                return DynamicFunctionMap::new;
            }
            else {
                return StaticFunctionMap::new;
            }
        };

        protected abstract Map<FunctionId, FunctionBundle> getFunctionBundlesById();

        protected abstract Map<FunctionId, FunctionMetadata> getFunctionsById();

        protected abstract Multimap<String, FunctionMetadata> getFunctionsByLowerCaseName();

        protected void verify()
        {
            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<String, Collection<FunctionMetadata>> entry : getFunctionsByLowerCaseName().asMap().entrySet()) {
                Collection<FunctionMetadata> values = entry.getValue();
                long aggregations = values.stream()
                        .map(FunctionMetadata::getKind)
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        @Override
        public List<FunctionMetadata> list()
        {
            return ImmutableList.copyOf(getFunctionsByLowerCaseName().values());
        }

        @Override
        public Collection<FunctionMetadata> get(String functionName)
        {
            return getFunctionsByLowerCaseName().get(functionName.toLowerCase(ENGLISH));
        }

        @Override
        public FunctionMetadata get(FunctionId functionId)
        {
            FunctionMetadata functionMetadata = getFunctionsById().get(functionId);
            checkArgument(functionMetadata != null, "Unknown function implementation: " + functionId);
            return functionMetadata;
        }

        @Override
        public FunctionBundle getFunctionBundle(FunctionId functionId)
        {
            FunctionBundle functionBundle = getFunctionBundlesById().get(functionId);
            checkArgument(functionBundle != null, "Unknown function implementation: " + functionId);
            return functionBundle;
        }
    }

    private static class StaticFunctionMap
            extends AbstractFunctionMap
    {
        private final Map<FunctionId, FunctionBundle> functionBundlesById;
        private final Map<FunctionId, FunctionMetadata> functionsById;
        // function names are currently lower cased
        private final Multimap<String, FunctionMetadata> functionsByLowerCaseName;

        public StaticFunctionMap()
        {
            functionBundlesById = ImmutableMap.of();
            functionsById = ImmutableMap.of();
            functionsByLowerCaseName = ImmutableListMultimap.of();
        }

        public StaticFunctionMap(AbstractFunctionMap map, FunctionBundle functionBundle)
        {
            this.functionBundlesById = ImmutableMap.<FunctionId, FunctionBundle>builder()
                    .putAll(map.getFunctionBundlesById())
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, functionMetadata -> functionBundle)))
                    .buildOrThrow();
            this.functionsById = ImmutableMap.<FunctionId, FunctionMetadata>builder()
                    .putAll(map.getFunctionsById())
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, Function.identity())))
                    .buildOrThrow();

            ImmutableListMultimap.Builder<String, FunctionMetadata> functionsByName = ImmutableListMultimap.<String, FunctionMetadata>builder()
                    .putAll(map.getFunctionsByLowerCaseName());
            functionBundle.getFunctions()
                    .forEach(functionMetadata -> functionsByName.put(functionMetadata.getSignature().getName().toLowerCase(ENGLISH), functionMetadata));
            this.functionsByLowerCaseName = functionsByName.build();

            verify();
        }

        @Override
        protected Map<FunctionId, FunctionBundle> getFunctionBundlesById()
        {
            return functionBundlesById;
        }

        @Override
        protected Map<FunctionId, FunctionMetadata> getFunctionsById()
        {
            return functionsById;
        }

        @Override
        protected Multimap<String, FunctionMetadata> getFunctionsByLowerCaseName()
        {
            return functionsByLowerCaseName;
        }

        @Override
        public void checkFunctionExists(FunctionMetadata functionMetadata)
        {
            for (FunctionMetadata existingFunction : list()) {
                checkArgument(!functionMetadata.getFunctionId().equals(existingFunction.getFunctionId()), "Function already registered: %s", functionMetadata.getFunctionId());
                checkArgument(!functionMetadata.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", functionMetadata.getSignature());
            }
        }
    }

    private static class DynamicFunctionMap
            extends AbstractFunctionMap
    {
        private final Map<FunctionId, FunctionBundle> functionBundlesById;
        private final Map<FunctionId, FunctionMetadata> functionsById;
        // function names are currently lower cased
        private final Multimap<String, FunctionMetadata> functionsByLowerCaseName;

        public DynamicFunctionMap()
        {
            functionBundlesById = ImmutableMap.of();
            functionsById = ImmutableMap.of();
            functionsByLowerCaseName = ImmutableListMultimap.of();
        }

        public DynamicFunctionMap(AbstractFunctionMap map, FunctionBundle functionBundle)
        {
            this.functionBundlesById = buildFunctionBundlesById(map, functionBundle);
            this.functionsById = buildFunctionsById(map, functionBundle);
            this.functionsByLowerCaseName = buildFunctionsByLowerCaseName(map, functionBundle);

            verify();
        }

        private Map<FunctionId, FunctionBundle> buildFunctionBundlesById(AbstractFunctionMap functionMap, FunctionBundle functionBundle)
        {
            Set<FunctionId> functionIdsToAdd = extractFunctionIdsFromFunctionBundle(functionBundle);

            Map<FunctionId, FunctionBundle> originalFunctionBundles = functionMap.getFunctionBundlesById()
                    .entrySet()
                    .stream()
                    .filter(e -> !functionIdsToAdd.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return ImmutableMap.<FunctionId, FunctionBundle>builder()
                    .putAll(originalFunctionBundles)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, functionMetadata -> functionBundle)))
                    .buildOrThrow();
        }

        private Map<FunctionId, FunctionMetadata> buildFunctionsById(AbstractFunctionMap functionMap, FunctionBundle functionBundle)
        {
            Set<FunctionId> functionIdsToAdd = extractFunctionIdsFromFunctionBundle(functionBundle);

            Map<FunctionId, FunctionMetadata> originalFunctionsById = functionMap.getFunctionsById()
                    .entrySet().stream().filter(e -> !functionIdsToAdd.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return ImmutableMap.<FunctionId, FunctionMetadata>builder()
                    .putAll(originalFunctionsById)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, Function.identity())))
                    .buildOrThrow();
        }

        private Multimap<String, FunctionMetadata> buildFunctionsByLowerCaseName(AbstractFunctionMap functionMap, FunctionBundle functionBundle)
        {
            Set<FunctionId> functionIdsToAdd = extractFunctionIdsFromFunctionBundle(functionBundle);

            ImmutableListMultimap.Builder<String, FunctionMetadata> functionsByName = ImmutableListMultimap.<String, FunctionMetadata>builder();
            functionMap.getFunctionsByLowerCaseName()
                    .entries()
                    .stream()
                    .filter(e -> !functionIdsToAdd.contains(e.getValue().getFunctionId())).forEach(e -> functionsByName.put(e.getKey(), e.getValue()));
            functionBundle.getFunctions()
                    .forEach(functionMetadata -> functionsByName.put(functionMetadata.getSignature().getName().toLowerCase(ENGLISH), functionMetadata));

            return functionsByName.build();
        }

        private Set<FunctionId> extractFunctionIdsFromFunctionBundle(FunctionBundle functionBundle)
        {
            return functionBundle.getFunctions().stream()
                    .map(FunctionMetadata::getFunctionId)
                    .collect(Collectors.toSet());
        }

        @Override
        protected Map<FunctionId, FunctionBundle> getFunctionBundlesById()
        {
            return functionBundlesById;
        }

        @Override
        protected Map<FunctionId, FunctionMetadata> getFunctionsById()
        {
            return functionsById;
        }

        @Override
        protected Multimap<String, FunctionMetadata> getFunctionsByLowerCaseName()
        {
            return functionsByLowerCaseName;
        }
    }

    @FunctionalInterface
    public interface FunctionEvictEventListener
    {
        void evict(FunctionId functionId);
    }
}
