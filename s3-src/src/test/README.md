## To test with maven

The maven plugin will assign random high port numbers to the containers that have been started.
```
mvn install
```

## To run from the command line or in a debugger

Make sure that the war is up to date
```
mvn install -Ddocker.skip -DskipITs -Dmaven.test.skip=true
```

If running on a desktop
```
export ECR_REGISTRY=it-docker-registry
```

Launch Containers
```
docker-compose -f s3-src/src/test/docker/docker-compose.yml up -d
```

Run the junit tests in VSCode

```
docker-compose -f s3-src/src/test/docker/docker-compose.yml down
```