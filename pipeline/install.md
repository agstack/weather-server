  
## Data Pipelines

### Prerequesites 

* [liminal](https://github.com/apache/incubator-liminal) 
* a local kubernetes cluster

### Getting started

``` bash
kubectl apply -f  storage.yml

liminal build
liminal deploy --clean
# in case of issue  see https://github.com/helm/charts/issues/23589
liminal start
```

Then go to http://localhost:8080 and launch the job.
 