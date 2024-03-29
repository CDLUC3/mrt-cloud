# Merritt Cloud Interface

This library is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc).

## Component Diagram

```mermaid
%%{init: {'theme': 'neutral', 'securityLevel': 'loose', 'themeVariables': {'fontFamily': 'arial'}}}%%
graph TD
  REPLIC(Replication)
  click REPLIC href "https://github.com/CDLUC3/mrt-replic" "source code"
  AUDIT(AUDIT - Fixity Check)
  click AUDIT href "https://github.com/CDLUC3/mrt-audit" "source code"
  ST(Storage)
  click ST href "https://github.com/CDLUC3/mrt-store" "source code"
  MRTCLOUD(mrt-cloud library)
  click MRTCLOUD href "https://github.com/CDLUC3/mrt-cloud" "source code"

  subgraph flowchart
    subgraph cloud_storage
      CLDS3[/AWS S3/]
      CLDSDSC[/SDSC Minio/]
      CLDWAS[/Wasabi/]
      CLDGLC[/Glacier/]
    end

    ST --> MRTCLOUD
    AUDIT --> MRTCLOUD
    REPLIC --> MRTCLOUD
    MRTCLOUD --> CLDS3
    MRTCLOUD --> CLDSDSC
    MRTCLOUD --> CLDWAS
    MRTCLOUD --> CLDGLC
  end
  style CLDS3 fill:#77913C
  style CLDGLC fill:#77913C
  style CLDSDSC fill:#77913C
  style CLDWAS fill:#77913C

  style MRTCLOUD stroke:red,stroke-width:4px
```

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
