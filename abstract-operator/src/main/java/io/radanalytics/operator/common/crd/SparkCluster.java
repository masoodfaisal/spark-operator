package io.radanalytics.operator.common.crd;

import io.fabric8.kubernetes.client.CustomResource;

public class SparkCluster<U> extends CustomResource {
    private U spec;
    private SparkStatus status;

    public SparkCluster() {
        this.status = new SparkStatus();
    }

    public SparkStatus getStatus() {
        return this.status;
    }

    public void setStatus(SparkStatus status) {
        this.status = status;
    }

    public U getSpec() {
        return spec;
    }

    // public void setSpec(U spec) {
    //     this.spec = spec;
    // }
}
