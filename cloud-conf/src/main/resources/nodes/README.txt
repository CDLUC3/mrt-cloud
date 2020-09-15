The NodeIO class handles a paramertized definition for all cloud IO members.

A NodeIO name is used to specifically identify which node list to use.

Each node.<seq> line in the list defines a node and cloud definitions for that node

This process is now used on all backend Merritt services.

********************************************************************
Example set:

./nodes-prod.properties
node.1=9001|sdsc|distrib.prod.9001.__
node.2=9103|sdsc|prod-9103
node.3=8001|ucla-prod
node.4=5001|aws-std|uc3-s3mrt5001-prd
node.5=6001|aws-near|uc3-s3mrt6001-prd
node.6=7001|aws-std|uc3-s3mrt1001-prd
node.7=7021|ch-riverside-prd|8100

* ch-riverside-prd
serviceType=cloudhost
base=https://uc3-mrtdat1-dev.cdlib.org:30443/cloudhost

./sdsc.properties
serviceType=swift
access_key=our_service:our_group
secret_key=pwd
host=cloud.sdsc.edu

./ucla-prod
serviceType=pairtree
base=/mrt-ucla-prod/dpr2/repository/node8001/store/fileCloud

./aws-std
serviceType=aws
accessMode=on-line
storageClass=Standard

./aws-near
serviceType=aws
accessMode=near-line

********************************************************************

Process module:
Repository: https://github.com/CDLUC3/mrt-cloud
Project: s3-src
Class: org.cdlib.mrt.s3.service.NodeIO

********************************************************************
SERVICE TYPES:

Swift
*node:
node.<seq>=<storage node>|<properties lookup name>|<swift container>

*Properties:
serviceType=swift
access_key=<Organization key:group key>
secret_key=<password>
host=<server>

*Example
node.2=9103|sdsc|prod-9103
...

./sdsc.properties
serviceType=swift
access_key=our_service:our_group
secret_key=pwd
host=cloud.sdsc.edu

-------------------------------------------------------------------
Pairtree
*node:
node.<seq>=<storage node>|<properties lookup name>

*Properties:
serviceType=pairtree
base=<base directory for tree and data>

*Example:
node.2=8100|csh-pair
...

./csh-pair.properties
serviceType=pairtree
base=./fileCloud

-------------------------------------------------------------------
AWS
*node:
node.<seq>=<storage node>|<properties lookup name>|<AWS bucket>


*Properties:
serviceType=aws
accessMode=
  "on-line" = directly accessible content
  "near-line" = Glacier
storageClass= optional AWS storage class
  "Standard" 
  "ReducedRedundancy"

*Example:
node.4=5001|aws-std|uc3-s3mrt5001-prd
...

./aws-std.properties
serviceType=aws
accessMode=on-line
storageClass=Standard

-------------------------------------------------------------------
Cloudhost 
*node:
node.<seq>=<storage node>|<properties lookup name>|<container number>

*Properties:
serviceType=cloudhost
base=<base url for remote cloudhost>

Example:
node.1=7301|ch-unm-test|8101
...

./ch-unm-test.properties
serviceType=cloudhost
base=http://oneshare-test.unm.edu:8080/cloudhost


********************************************************************
Example set:

./nodes-prod.properties
node.1=9001|sdsc|distrib.prod.9001.__
node.2=9103|sdsc|prod-9103
node.3=8001|ucla-prod
node.4=5001|aws-std|uc3-s3mrt5001-prd
node.5=6001|aws-near|uc3-s3mrt6001-prd
node.6=7001|aws-std|uc3-s3mrt1001-prd
node.7=7021|ch-riverside-prd|8100

* ch-riverside-prd
serviceType=cloudhost
base=https://uc3-mrtdat1-dev.cdlib.org:30443/cloudhost

./sdsc.properties
serviceType=swift
access_key=our_service:our_group
secret_key=pwd
host=cloud.sdsc.edu

./ucla-prod
serviceType=pairtree
base=/mrt-ucla-prod/dpr2/repository/node8001/store/fileCloud

./aws-std
serviceType=aws
accessMode=on-line
storageClass=Standard

./aws-near
serviceType=aws
accessMode=near-line

********************************************************************
