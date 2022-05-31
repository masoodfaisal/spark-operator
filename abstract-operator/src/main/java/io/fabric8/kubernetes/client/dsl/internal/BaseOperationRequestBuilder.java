package io.fabric8.kubernetes.client.dsl.internal;



import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.dsl.base.BaseOperation;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import io.fabric8.kubernetes.client.utils.URLUtils;
import okhttp3.HttpUrl;
import okhttp3.Request;

class BaseOperationRequestBuilder<T extends HasMetadata, L extends KubernetesResourceList<T>> {
    private final URL requestUrl;
    private final BaseOperation<T, L, ?> baseOperation;
    private final ListOptions listOptions;

    public BaseOperationRequestBuilder(BaseOperation<T, L, ?> baseOperation, ListOptions listOptions) throws MalformedURLException {
        System.out.println("Exisitng plural: " + baseOperation.getPlural());
        System.out.println("new plural: " + listOptions.getKind().toLowerCase());
        this.baseOperation = baseOperation;

        URL tempRequestUrl = baseOperation.getNamespacedUrl();
        System.out.println("123123123123" + tempRequestUrl);
        this.requestUrl =  new URL(URLUtils.join(tempRequestUrl.toString().substring(0, tempRequestUrl.toString().lastIndexOf("/")), listOptions.getKind().toLowerCase().concat("s")));
        System.out.println("NEWNEWNEWN123123123123" + this.requestUrl);
        this.listOptions = listOptions;
    }

    public BaseOperation<T, L, ?> getBaseOperation() {
        return baseOperation;
    }

    public Request build(final String resourceVersion) {
        System.out.println("324234234234234234234" + resourceVersion);
        HttpUrl.Builder httpUrlBuilder = HttpUrl.get(requestUrl).newBuilder();

        listOptions.setResourceVersion(resourceVersion);
        HttpClientUtils.appendListOptionParams(httpUrlBuilder, listOptions);
        System.out.println("456456456678678678678678" + httpUrlBuilder);

        String origin = requestUrl.getProtocol() + "://" + requestUrl.getHost();
        if (requestUrl.getPort() != -1) {
            origin += ":" + requestUrl.getPort();
        }

        Request.Builder requestBuilder = new Request.Builder()
                .get()
                .url(httpUrlBuilder.build())
                .addHeader("Origin", origin);

        Config config = baseOperation.getConfig();
        if (Objects.nonNull(config)) {
            Map<String, String> customHeaders = config.getCustomHeaders();
            if (Objects.nonNull(customHeaders) && !customHeaders.isEmpty()) {
                for (String key : customHeaders.keySet()) {
                    requestBuilder.addHeader(key, customHeaders.get(key));
                }
            }
        }

        System.out.println("567567567567567567" + requestBuilder.build().url());

        return requestBuilder.build();
    }
}