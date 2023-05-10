# Downloading a Box Folder
## Approach
This mechanism allows the authorization for downloading a BoxFolder.

#### Box Developer App
- create a box developer app using App Token (Server Authentication)
  - this requires box administration authorization for the app (sent automatically)
- from the Configuration tab screen generate a Private Token - this is treated by Box as an AuthToken on the part of the app
  - be sure to copy this value

#### Remote source user
- Request remote user of content you want to download to add the developer app as a collaborator on the directory to be downloaded
- the developer app email to be used is in the developer App/configuration/ServiceAccountInfo - Service ID
- Request remote user to generate a shared link on the specific Box Folder to be downloaded

_Notes_
- The collaborator level needs to have download privileges. Restricting this level provides additional protection from any intentional or accidental damage
- Similarly the shared link only needs to have group level to include collaborators


#### Running Download
This directory is used for running downloading content from remote box hosts.

## Setup
Each download has a directory on /apps/dpr2/ingest_home/queue/boxContent specific to the download.

Download directory examples include .../2_2_2_screenshots, vaccine_screenshots

For each download directory will eventually contain 3 directories and 1 file:
- _boxconfig.prop_ - configuration information file needed to run download
- _data_ - contains the downloaded box files
- _meta_ - contains downloaded json metadata. The file and name of the metadata corresponds to the data name.
- _log_ - includes TLogs the json metadata downloaded for each file

## boxconfig.prop

### Example properties
~~~
privateAccess={!SSM: /uc3/mrt/stg/box/config/privateAccess !DEFAULT: none}
downloadURL=https://ucsf.box.com/s/gpi9li5ezzryxpdcc7iu92wu0g4u3n8c
dumpFreq=200
skipToName=AK/AK-20201215-000104.png
~~~
### Description properties
- privateAccess=the SSM key for private access key take from a box developers app using App Token (Server Authentication)
- downloadURL=shared download URL for the BoxFolder to be used for download. The link needs public authorization to include only local contributors
- dumpFreq=number of log outputs between processing state output. Is used to dump additional stats into the logs on a regular basis
- skipToName(optional)=skip to this key and begin processing the next key

_Note_  
Before this process can work - the developer app email needs to be added as a view contributor for this specific
directory. The developer app emai name is available under General Settings as the Service Account ID.

## data - download directory
The data direcotry contains the box file downloadse. All directory levels are maintained

## meta - Box properites
The meta directory corresponds to the data directory. The Box properties are saved as a JSON file

### Example JSON metadata download
~~~
{
    "id":"912008737752",
    "name":"AK-tertiary-20210602-120754.png",
    "pathName":"AK/AK-tertiary-20210602-120754.png"
    "sha1":"f5321d75200e5e1fd0d3692fb54d270eef0f9d2c"
    "size":19349,
    "status":"ok",
    "processMs":1169
}
~~~
### Description Download
- id=box id for file
- name=box name or file name
- pathName=contains file levels plus box name
- sha1=sha1 digest
- size=byte size
- status=download status
- processMs=download milliseconds

## running download
From ~/ingest_home/queue/boxContent/boxrun

Issue
./runbox.sh vaccine_screenshots

This looks for /apps/dpr2/ingest_home/queue/boxContent/vaccine_screenshots to begin processing.

## logs
The logs contain a header with the json box metadata.

Example log
~~~
Mon May 08 21:43:28 PDT 2023    (08) [box] {"id":"916869257600","name":"WY-tertiary-20210602-134900.png","pathName":"WY/WY-tertiary-20210602-134900.png","sha1":"1af84cc47b07483a19ce9a3e9666a8f9aeeb517f","size":299524,"status":"ok","processMs":1412}
~~~

Contains the time for processing followed by the JSON metadata included in the meta file.

# Running
In order to run this process an executable jar needs to be created for org.cdlib.mrt.box.action.DownloadBoxMeta.

## Creating executable jar
- cd mrt-cloud/box-src
<br>--go to mrt-cloud repositoy box-src
- mvn clean install
<br>--build with maven
- cd mrt-cloud/box-run
<br>--go to mrt-cloud repositoy box-run
- mvn clean install
<br>--build runtime jar with maven
- cp ~/.m2/repository/org/cdlib/mrt/mrt-boxrun/1.0/mrt-boxrun-1.0-jar-with-dependencies.jar run.jar
<br>--copy runtime jar from local .m2 repository to executable directory

## Running jar
### Example script
~~~
export BOXRUN=./230428_test
java -jar run.jar
~~~

- BOXRUN is set to the download directory (above)
- java -jar run.jar - execute constructed jar
