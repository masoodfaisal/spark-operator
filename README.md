# spark-operator

[![Build status](https://travis-ci.org/Jiri-Kremser/spark-operator.svg?branch=master)](https://travis-ci.org/Jiri-Kremser/spark-operator)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

`ConfigMap`-based approach for managing the Spark clusters in Kubernetes and OpenShift.

# Quick Start

Run the `spark-operator` deployment:
```bash
kubectl create -f manifest/operator.yaml
```

Create new cluster from the prepared example:

```bash
kubectl create -f examples/cluster.yaml
```

After issuing the commands above, you should be able to see a new Spark cluster running in the current namespace.

```bash
kubectl get pods
NAME                               READY     STATUS    RESTARTS   AGE
my-spark-cluster-m-1-5kjtj         1/1       Running   0          10s
my-spark-cluster-w-1-m8knz         1/1       Running   0          10s
my-spark-cluster-w-1-vg9k2         1/1       Running   0          10s
spark-operator-510388731-852b2     1/1       Running   0          27s
```

Once you don't need the cluster anymore, you can delete it by deleting the config map resource by:
```bash
kubectl delete cm my-spark-cluster
```

# Very Quick Start

```bash
# create operator
kubectl create -f http://bit.ly/sparkop

# create cluster
cat <<EOF | kubectl create -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-cluster
  labels:
    radanalytics.io/kind: cluster
data:
  config: |-
    workerNodes: "2"
EOF
```

### OpenShift

For deployment on OpenShift use the same commands as above, but with `oc` instead of `kubectl`.

### Demo
<!--
asciinema rec -i 3
docker run -\-rm -v $PWD:/data asciinema/asciicast2gif -s 1.18 -w 104 -h 27 -t monokai 189204.cast demo.gif
-->
[![Watch the full asciicast](./ascii.gif)](https://asciinema.org/a/189204?&cols=104&rows=27&theme=monokai)

### Images
[![Layers info](https://images.microbadger.com/badges/image/jkremser/spark-operator.svg)](https://microbadger.com/images/jkremser/spark-operator)
`jkremser/spark-operator:latest`

[![Layers info](https://images.microbadger.com/badges/image/jkremser/spark-operator:centos-latest.svg)](https://microbadger.com/images/jkremser/spark-operator:centos-latest)
`jkremser/spark-operator:centos-latest`