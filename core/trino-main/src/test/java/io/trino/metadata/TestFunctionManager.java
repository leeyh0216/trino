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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.FunctionType;
import org.testng.annotations.Test;

import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.spi.function.FunctionId.toFunctionId;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestFunctionManager
{
    @Test
    public void testEvict()
    {
        ResolvedFunction customAddFunction = new ResolvedFunction(
                new BoundSignature("custom_add", BIGINT, ImmutableList.of(BIGINT, BIGINT)),
                GlobalSystemConnector.CATALOG_HANDLE,
                toFunctionId(Signature.builder()
                        .name("custom_add")
                        .returnType(BIGINT)
                        .argumentType(new TypeSignature("bigint"))
                        .argumentType(new TypeSignature("bigint"))
                        .build()),
                SCALAR,
                true,
                new FunctionNullability(false, ImmutableList.of(false, false)),
                ImmutableMap.of(),
                ImmutableSet.of());

        FunctionBundle functionBundle = extractFunctions(TestGlobalFunctionCatalog.CustomAdd.class);
        FunctionConfig dynamicFunctionConfig = new FunctionConfig();
        dynamicFunctionConfig.setDynamicFunctionLoading(true);
        GlobalFunctionCatalog functionCatalog = new GlobalFunctionCatalog(dynamicFunctionConfig);
        functionCatalog.addFunctions(functionBundle);

        FunctionManager functionManager = new FunctionManager(CatalogServiceProvider.fail(), functionCatalog);
        functionManager.getScalarFunctionImplementation(customAddFunction, getInvocationConvention(customAddFunction.getSignature(), customAddFunction.getFunctionNullability()));
        assertEquals(1, functionManager.getSpecializedScalarCache().size());
        for (FunctionMetadata metadata : functionBundle.getFunctions()) {
            functionManager.evict(metadata.getFunctionId());
        }
        assertEquals(0, functionManager.getSpecializedScalarCache().size());
    }

    //from InterpretedFunctionInvoker
    private static InvocationConvention getInvocationConvention(BoundSignature signature, FunctionNullability functionNullability)
    {
        ImmutableList.Builder<InvocationConvention.InvocationArgumentConvention> argumentConventions = ImmutableList.builder();
        for (int i = 0; i < signature.getArgumentTypes().size(); i++) {
            Type type = signature.getArgumentTypes().get(i);
            if (type instanceof FunctionType) {
                argumentConventions.add(FUNCTION);
            }
            else if (functionNullability.isArgumentNullable(i)) {
                argumentConventions.add(BOXED_NULLABLE);
            }
            else {
                argumentConventions.add(NEVER_NULL);
            }
        }

        return new InvocationConvention(
                argumentConventions.build(),
                functionNullability.isReturnNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                true,
                true);
    }
}
