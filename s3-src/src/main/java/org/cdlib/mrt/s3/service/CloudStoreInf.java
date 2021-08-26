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
import org.cdlib.mrt.cloud.object.StateHandler;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Identifier;

/**
 * Interface for Cloud Store
 * @author dloy
 */
public interface CloudStoreInf
{    
    public static enum CloudAPI{AWS_S3, SDSC_SWIFT, CLOUDHOST, PAIRTREE, STORE};
    
    /**
     * Upload cloud object
     * @param bucketName s3 bucket - rackspace container
     * @param key entry key to be added
     * @param inputFile file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putObject(
            String bucketName,
            String key,
            File inputFile)
        throws TException;
    
    /**
     * Upload cloud object
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @param inputFile file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile)
        throws TException;
    
    /**
     * Upload cloud manifest
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @param inputFile manifest file to be uploaded
     * @return information about upload
     * @throws TException 
     */
    public CloudResponse putManifest(
            String bucketName,
            Identifier objectID,
            File inputFile)
        throws TException;

    /**
     * Delete object
     * @param bucketName s3 bucket - rackspace container
     * @param key cloud object key
     * @return
     * @throws TException 
     */
    public CloudResponse deleteObject (
            String bucketName,
            String key)
        throws TException;
    
    /**
     * Delete cloud object
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @return
     * @throws TException 
     */
    public CloudResponse deleteObject (
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException;
    
    /**
     * Delete cloud manifest
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @return
     * @throws TException 
     */
    public CloudResponse deleteManifest (
            String bucketName,
            Identifier objectID)
        throws TException;
    
    /**
     * Retrieve cloud Object
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @param versionID Version identifier
     * @param fileID File/component identifier that may contain a directory element
     * @param response input stream of retrieved object
     * @return
     * @throws TException 
     */
    public InputStream getObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            CloudResponse response)
        throws TException;

    /**
     * Retrieve cloud object
     * @param bucketName s3 bucket - rackspace container
     * @param key cloud object key for object to be returned
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public InputStream getObject(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException;
    
    /**
     * Retrieve cloud object in streaming mode
     * @param bucketName s3 bucket - rackspace container
     * @param key cloud object key for object to be returned
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public InputStream getObjectStreaming(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException;
    
    /**
     * Retrieve content into a file
     * @param bucketName s3 bucket - rackspace container
     * @param key cloud object key for object to be returned
     * @param outFile file to receive content
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public void getObject(
            String bucketName,
            String key,
            File outFile,
            CloudResponse response)
        throws TException;

    /**
     * Retrieve metadata for file
     * @param bucketName s3 bucket - rackspace container
     * @param fileKey cloud object key for object to be returned
     * @return exists: named properties; does not exist: null
     * @throws TException 
     */
    public Properties getObjectMeta(
            String bucketName,
            String fileKey)
        throws TException;
    
    /**
     * Retrieve cloud manifest
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @param response
     * @return input stream of retrieved object
     * @throws TException 
     */
    public InputStream getManifest(
            String bucketName,
            Identifier objectID,
            CloudResponse response)
        throws TException;

    /**
     * Retrieve cloud manifest
     * @param bucketName s3 bucket - rackspace container
     * @param objectID Object identifier
     * @return input stream of retrieved manifest
     * @throws TException 
     */
    public InputStream getManifest(
            String bucketName,
            Identifier objectID)
        throws TException;

    /**
     * Restore nearline content
     * @param containers3 bucket - rackspace container
     * @param key cloud key
     * @param outFile target file of response
     * @param response
     * @throws TException 
     */
    public void restoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException;
    
    /**
     * Return content that matches for the length of the key
     * @param bucketName s3 bucket - rackspace container
     * @param key prefix of content to be returned
     * @return CloudResponse with list of matching entries
     * @throws TException 
     */
    public CloudResponse getObjectList (
            String bucketName,
            String key)
        throws TException;

    /**
     * Return a set number of entry metadata based on start key
     * @param bucketName s3 bucket - rackspace container
     * @param key content key of the first output
     * @param limit number of returned entries
     * @return CloudResponse with list of sequential entries
     * @throws TException 
     */
    public CloudResponse getObjectList (
            String bucketName,
            String key,
            int limit)
        throws TException;
    
    /**
     * Return info on bucket/container
     * @param bucketName s3 bucket - rackspace container
     * @return
     * @throws TException 
     */
    public CloudResponse getObjectList (
            String bucketName)
        throws TException;
    
    /**
     * Return list of S3 keys after a start key
     * @param bucketName s3 bucket
     * @param afterKey start return list after this value
     * @param limit number of keys to return
     * @return cloud response with keys
     * @throws TException 
     */
    public CloudResponse getObjectListAfter (
            String bucketName,
            String afterKey,
            int limit)
        throws TException;
    
    /**
     * Return state of cloud store manager
     * @param bucketName bucket container
     * @return
     * @throws TException 
     */
    public StateHandler.RetState getState (
            String bucketName)
        throws TException;

    /**
     * Retrieve cloud properties for bucket
     * @return 
     */
    public Properties getCloudProp();
    
    /**
     * Validate that the md5 digest matches cloud object digest
     * @param bucketName s3 bucket - rackspace container
     * @param key cloud object key
     * @param inMd5 md5 digest value
     * @return
     * @throws TException 
     */
    public CloudResponse validateMd5(String bucketName, String key, String inMd5)
        throws TException;
    
    /**
     * Generic validation of stored content
     * @param bucketName cloud container
     * @param key cloud key
     * @param digest expected Digest and Digest Type
     * @param length expected file length
     * @return
     * @throws TException 
     */
    public CloudResponse validateDigest(String bucketName, String key, MessageDigest digest, long length)
        throws TException;
    
    /**
     * Determine if host:port  is available
     * @param testUrlS - base url for site to be tested
     * @return true=alive; false=not alive; null=test not performed
     */
    public Boolean isAlive(String bucketName);
    
    /**
     * Returns an S3 presigned URL
     * @param expirationMinutes minutes for the signed URL to work
     * @param bucketName cloud container
     * @param key cloud key
     * @param contentType optional ContentType for this file
     * @param contentDisp optional ContentDisposition for this file
     * @return CloudResponse where response.getReturnURL returns the presigned URL
     * @throws TException
     */
    public CloudResponse getPreSigned (
            long expirationMinutes,
            String bucketName,
            String key,
            String contentType,
            String contentDisp)
        throws TException;
    /**
     * Return part of a cloud object as stream
     * @param bucketName cloud container
     * @param key cloud key
     * @param start byte location to start from zero
     * @param stop byte location to stop from zero
     * @param response Res[pmse tp reqiest
     * @return input stream for segment
     * @throws TException 
     */
    public InputStream getRangeStream(
            String bucketName,
            String key,
            long start,
            long stop,
            CloudResponse response)
        throws TException;
    
    /**
     * Return the API type
     * @return AWS-S3, SDSC-Swift, Cloudhost, Pairtree
     */
    public CloudAPI getType();
    
    /**
     * Because of earlier SDSC bug only alpha-numerics could be used in a key.
     * true=alpha-numeric key, false=ASCII (current default)
     * @return 
     */
    public boolean isAlphaNumericKey(); 
}

