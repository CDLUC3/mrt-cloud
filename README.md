# Merritt Cloud Interface

This library is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc).

## Component Diagram

[![Component Diagram](https://github.com/CDLUC3/mrt-doc/raw/main/diagrams/mrt-cloud.mmd.svg)](https://cdluc3.github.io/mrt-doc/diagrams/mrt-cloud)

## Purpose

This library abstracts the operations performed across the cloud storage providers utilized by the Merritt Preservation System.

A [Yaml Configuration File](cloud-conf/src/main/resources/yaml/cloudConfig.yml) associates a **storage node** with a specific cloud provider.

## Used By

This code is used by
- [Merritt Storage](https://github.com/CDLUC3/mrt-store)
- [Merritt Audit](https://github.com/CDLUC3/mrt-audit)
- [Merritt Replic](https://github.com/CDLUC3/mrt-replic)

## For external audiences
This code is not intended to be run apart from the Merritt Preservation System.

See [Merritt Docker](https://github.com/CDLUC3/merritt-docker) for a description of how to build a test instnce of Merritt.

## Build instructions
This code is bundled into microservice WAR files for deployment.

## Test instructions

## Internal Links

### Deployment and Operations at CDL

See the implementing microservices for details.
