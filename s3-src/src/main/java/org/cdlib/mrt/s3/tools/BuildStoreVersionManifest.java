package org.cdlib.mrt.s3.tools;

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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * this routine is used to build a manifest.xml file from content saved in cloud.
 * 
 * Note that the constructed manifest.xml will only contain additions and content replacement. 
 * If any component was deleted in later versions it cannot be identified by this routine.
 */
public class BuildStoreVersionManifest {
    
    protected static final String NAME = "BuildStoreVersionManifest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected final static String NL = System.getProperty("line.separator");
    
    protected final NodeService service;
    protected final LoggerInf logger;
     
    public static void main(String[] args) 
            throws IOException,TException 
    {
        try {
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            String nodeName = "nodes-m538";
            String manDirS = "/apps/replic/prod/tasks/160901-july-fix/repository/manifests";
            File manDir = new File(manDirS);
            String arkS = "ark:/13030/qt1d12m538";
            long node4001 = 4001;
            NodeService service9001 = NodeService.getNodeService(nodeName, 9001, logger);
            //NodeService service4001 = NodeService.getNodeService(nodeName, node4001, logger);
            
            BuildStoreVersionManifest bsvm = getBuildStoreVersionManifest(service9001, logger);
            //NodeService service8001 = NodeService.getNodeService(nodeName, node8001, logger);
            File retFile = bsvm.build(arkS, manDir, "http://store01-aws.cdlib.org:35121/content/" + 9001);
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
     
    public static void main_qt8vx8f6w6(String[] args) 
            throws IOException,TException 
    {
        try {
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            String nodeName = "nodes-prod-temp";
            String manDirS = "/apps/replic/test/store/160801-validate/repository/manifests9001";
            File manDir = new File(manDirS);
            String arkS = "ark:/13030/qt8vx8f6w6";
            long node4001 = 4001;
            NodeService service9001 = NodeService.getNodeService(nodeName, 9001, logger);
            //NodeService service4001 = NodeService.getNodeService(nodeName, node4001, logger);
            
            BuildStoreVersionManifest bsvm = getBuildStoreVersionManifest(service9001, logger);
            //NodeService service8001 = NodeService.getNodeService(nodeName, node8001, logger);
            File retFile = bsvm.build(arkS, manDir, "http://store01-aws.cdlib.org:35121/content/" + 9001);
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
     
    public static void main_4001(String[] args) 
            throws IOException,TException 
    {
        try {
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            String nodeName = "nodes-prod-temp";
            String manDirS = "/apps/replic/test/store/160801-validate/repository/manifests";
            File manDir = new File(manDirS);
            String arkS = "ark:/13030/qt8vx8f6w6";
            long node4001 = 4001;
            //NodeService service9001 = NodeService.getNodeService(nodeName, node9001, logger);
            NodeService service4001 = NodeService.getNodeService(nodeName, node4001, logger);
            
            BuildStoreVersionManifest bsvm = getBuildStoreVersionManifest(service4001, logger);
            //NodeService service8001 = NodeService.getNodeService(nodeName, node8001, logger);
            File retFile = bsvm.build(arkS, manDir, "http://uc3-mrtreplic2-dev.cdlib.org:35121/content/" + node4001);
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    /**
     * Get object for constructing manifest
     * @param cloud Cloud access object interface
     * @param container container/bucket of cloud storage containing object
     * @param directory local directory file to contain extracted content from cloud
     * @param logger Merritt logger
     * @returnBuildObjectManifest
     * @throws TException 
     */
    public static BuildStoreVersionManifest getBuildStoreVersionManifest(
            NodeService service,
            LoggerInf logger)
        throws TException
    {
        return new BuildStoreVersionManifest( service, logger);
    }
    
    
    /**
     * Constructor
     * @param cloud Cloud access object interface
     * @param container container/bucket of cloud storage containing object
     * @param directory local directory file to contain extracted content from cloud
     * @param logger Merritt logger
     * @throws TException 
     */
    protected BuildStoreVersionManifest(
            NodeService service,
            LoggerInf logger)
        throws TException
    {
        this.service = service;
        this.logger = logger; 
    }
    
    /**
     * Build manifest.xml
     * @param objectIDS
     * @return File containing manifest.xml
     * @throws TException 
     */
    public File build(
            String objectIDS,
            File outDir,
            String fileURLS)
        throws TException
    {
        try {
            Identifier objectID = new Identifier(objectIDS);
            VersionMap map = getVersionMap(objectID);
            if (map == null) {
                System.out.println("No map found");
                return null;
            } 
            int current = map.getCurrent();
            System.out.println("Map found:" + current);
            for (int version=1; version <= current; version++) {
                File addManifest = new File(outDir, "manifest-" + version + ".txt");
                System.out.println("Process:" + addManifest.getCanonicalPath());
                map.buildAddManifest(fileURLS, version, addManifest);
            }
            return outDir;
            
        } catch (TException tex) {
            System.out.println("Exception:" + tex);
            tex.printStackTrace();
            throw tex;
                    
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }  protected VersionMap getVersionMap(Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = service.getManifest(objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}
