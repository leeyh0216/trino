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
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ForNodeManager;
import io.trino.server.FunctionPluginLoadRequest;
import io.trino.server.InternalCommunicationConfig;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LoadFunction;
import io.trino.tx.Transaction;

import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

public class LoadFunctionTask
        implements DataDefinitionTask<LoadFunction>
{
    private static final Logger log = Logger.get(LoadFunctionTask.class);
    private static final JsonCodec<FunctionPluginLoadRequest> FUNCTION_PLUGIN_LOAD_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(FunctionPluginLoadRequest.class);

    private final ServiceSelector serviceSelector;
    private final boolean httpsRequired;
    private final HttpClient httpClient;

    @Inject
    public LoadFunctionTask(
            @ServiceType("trino") ServiceSelector serviceSelector,
            @ForNodeManager HttpClient httpClient,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        this.serviceSelector = serviceSelector;
        this.httpClient = httpClient;
        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
    }

    @Override
    public String getName() {
        return "LOAD FUNCTION";
    }

    @Override
    public ListenableFuture<Void> execute(LoadFunction statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector) {
        Transaction tx = new Transaction();

        Set<ServiceDescriptor> online = new HashSet<>(serviceSelector.selectAllServices());

        for(ServiceDescriptor service : online)
        {
            URI uri = getHttpUri(service);
            if(uri == null) {
                continue;
            }

            uri = uriBuilderFrom(uri).appendPath("/v1/function").build();

            URI finalUri = uri;
            tx.addDistTx(new Transaction.DistTx(uri.toString(), () -> {
                Request request = preparePost()
                        .setUri(finalUri)
                        .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                        .setBodyGenerator(jsonBodyGenerator(FUNCTION_PLUGIN_LOAD_REQUEST_JSON_CODEC, new FunctionPluginLoadRequest(statement.getPluginName().toString(), statement.getPluginDir())))
                        .build();
                return httpClient.execute(request, new ResponseHandler<>() {
                    @Override
                    public ListenableFuture<Void> handleException(Request request, Exception exception) throws RuntimeException {
                        return immediateFailedFuture(exception);
                    }

                    @Override
                    public ListenableFuture<Void> handle(Request request, Response response) throws RuntimeException {
                        if(response.getStatusCode() != 200)
                            return immediateFailedFuture(new Exception("Invalid return code: " + response.getStatusCode()));
                        else
                            return immediateVoidFuture();
                    }
                });
            }, () -> {
                return immediateVoidFuture();
            }, () -> {
                return immediateVoidFuture();
            }));
        }
        tx.start();
        return immediateVoidFuture();
    }

    private URI getHttpUri(ServiceDescriptor descriptor)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }
}
