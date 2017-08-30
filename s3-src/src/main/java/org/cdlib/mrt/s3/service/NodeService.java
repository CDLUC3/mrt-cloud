/*
Copyright (c) 2005-2010, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/
package org.cdlib.mrt.s3.service;

import org.cdlib.mrt.core.MessageDigest;
import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Identifier;

/**
 * Interface for Cloud Store
 * @author dloy
 */
public class NodeService
{    
    protected static final String NAME = "NodeService";
    protected static final String MESSAGE = NAME + ": ";
    protected NodeIO.AccessNode cloudNode = null;
    protected CloudStoreInf service = null;
    protected String nodeName = null;
    protected String bucket = null;
    protected long node = 0;
    protected LoggerInf logger = null;
    
    public static NodeService getNodeService(String nodeName, long node, LoggerInf logger)
        throws TException
    {
        return new NodeService(nodeName, node, logger);
    }
    
    public static NodeService getNodeService(NodeIO nodes, long node, LoggerInf logger)
        throws TException
    {
        NodeIO.AccessNode cloudNode = nodes.getAccessNode(node);
        if (cloudNode == null) return null;
        return new NodeService(cloudNode, node, logger);
    }
    
    public NodeService(String nodeName, long node, LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        this.nodeName = nodeName;
        cloudNode = NodeIO.getCloudNode(nodeName, node, logger);
        if (cloudNode == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "node not found:" + node);
        }
        service = cloudNode.service;
        bucket = cloudNode.container;
        this.node = node;
        System.out.println(cloudNode.dump("NodeService"));
    }
    
    public NodeService(NodeIO.AccessNode cloudNode, long node, LoggerInf logger)
        throws TException
    {
        this.cloudNode = cloudNode;
        if (cloudNode == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "node not found:" + node);
        }
        service = cloudNode.service;
        bucket = cloudNode.container;
        this.node = node;
        System.out.println(cloudNode.dump("NodeService"));
    }
    
    public Properties getServiceProperties()
    {
        Properties serviceProp = new Properties();
        serviceProp.setProperty("nodeName", nodeName);
        serviceProp.setProperty("node", "" + cloudNode.nodeNumber);
        serviceProp.setProperty("container", cloudNode.container);
        serviceProp.setProperty("serviceType", cloudNode.serviceType);
        return serviceProp;
    }
    
    /**
     * Upload cloud object
     * @param key entry key to be added
     * @param inputFile file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putObject(
            String key,
            File inputFile)
        throws TException
    {
        return service.putObject(bucket, key, inputFile);
    }
    
    /**
     * Upload cloud object
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @param inputFile file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putObject(
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile)
        throws TException
    {
        return service.putObject(bucket, objectID, versionID, fileID, inputFile);
    }
    
    /**
     * Upload cloud manifest
     * @param objectID Object identifier
     * @param inputFile manifest file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putManifest(
            Identifier objectID,
            File inputFile)
        throws TException
    {
        return service.putManifest(bucket, objectID, inputFile);
    }

    /**
     * Delete object
     * @param key cloud object key
     * @return
     * @throws TException 
     */
    public CloudResponse deleteObject (
            String key)
        throws TException
    {
        return service.deleteObject(bucket, key);
    }
    
    /**
     * Delete cloud object
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @return
     * @throws TException 
     */
    public CloudResponse deleteObject (
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        return service.deleteObject(bucket, objectID, versionID, fileID);
    }
    
    /**
     * Delete cloud manifest
     * @param objectID Object identifier
     * @return
     * @throws TException 
     */
    public CloudResponse deleteManifest (
            Identifier objectID)
        throws TException
    {
        return service.deleteManifest(bucket, objectID);
    }
    
    /**
     * Retrieve cloud Object
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @param response input stream of retrieved object
     * @return
     * @throws TException 
     */
    public InputStream getObject(
            Identifier objectID,
            int versionID,
            String fileID,
            CloudResponse response)
        throws TException
    {
        return service.getObject(bucket, objectID, versionID, fileID, response);
    }

    /**
     * Retrieve cloud object
     * @param key cloud object key for object to be returned
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public InputStream getObject(
            String key,
            CloudResponse response)
        throws TException
    {
        return service.getObject(bucket, key, response);
    }

    /**
     * Retrieve content into a file
     * @param key cloud object key for object to be returned
     * @param outFile file to receive content
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public void getObject(
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        service.getObject(bucket, key, outFile, response);
    }

    /**
     * Retrieve nearline content into a file
     * @param key cloud object key for object to be returned
     * @param outFile file to receive content
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public void restoreObject(
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        service.restoreObject(bucket, key, outFile, response);
    }
      

    /**
     * Retrieve metadata for file
     * @param bucketName s3 bucket - rackspace container
     * @param fileKey cloud object key for object to be returned
     * @return exists: named properties; does not exist: null
     * @throws TException 
     */
    public Properties getObjectMeta(
            String key)
        throws TException
    {
        return service.getObjectMeta(bucket, key);
    }
    
    /**
     * Retrieve cloud manifest
     * @param objectID Object identifier
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public InputStream getManifest(
            Identifier objectID,
            CloudResponse response)
        throws TException
    {
        return service.getManifest(bucket, objectID, response);
    }

    /**
     * Retrieve cloud manifest
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @return input stream of retrieved manifest
     * @throws TException 
     */
    public InputStream getManifest(
            Identifier objectID)
        throws TException
    {
        return service.getManifest(bucket, objectID);
    }

    /**
     * Return content that matches for the length of the key
     * @param key prefix of content to be returned
     * @return CloudResponse with list of matching entries
     * @throws TException 
     */
    public CloudResponse getObjectList (
            String key)
        throws TException
    {
        return service.getObjectList(bucket, key);
    }

    /**
     * Return a set number of entry metadata based on start key
     * @param key content key of the first output
     * @param limit number of returned entries
     * @return CloudResponse with list of sequential entries
     * @throws TException 
     */
    public CloudResponse getObjectList (
            String key,
            int limit)
        throws TException
    {
        return service.getObjectList(bucket, key, limit);
    }
    
    /**
     * Validate that the md5 digest matches cloud object digest
     * @param key cloud object key
     * @param inMd5 md5 digest value
     * @return
     * @throws TException 
     */
    public CloudResponse validateMd5(String key, String inMd5)
        throws TException
    {
        return service.validateMd5(bucket, key, inMd5);
    }
    
    /**
     * Generic validation of stored content
     * @param key cloud key
     * @param digest expected Digest and Digest Type
     * @param length expected file length
     * @return
     * @throws TException 
     */
    public CloudResponse validateDigest(String key, MessageDigest digest, long length)
        throws TException
    {
        return service.validateDigest(bucket, key, digest, length);
    }
    
    /**
     * Because of earlier SDSC bug only alpha-numerics could be used in a key.
     * true=alpha-numeric key, false=ASCII (current default)
     * @return 
     */
    public boolean isAlphaNumericKey()
    {
        return service.isAlphaNumericKey();
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getBucket() {
        return bucket;
    }

    public long getNode() {
        return node;
    }
    
    public CloudStoreInf getCloudService() {
        return service;
    }
    
    public String getServiceType() {
        return cloudNode.serviceType;
    }

    public LoggerInf getLogger() {
        return logger;
    }
    
}

