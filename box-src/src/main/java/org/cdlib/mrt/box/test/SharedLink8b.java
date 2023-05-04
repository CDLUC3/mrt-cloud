/*
Copyright (c) 2005-2012, Regents of the University of California
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
**********************************************************/
/**
 *
 * @author dloy
 */
package org.cdlib.mrt.box.test;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxSharedLink;
import com.box.sdk.Metadata;
import com.box.sdk.sharedlink.BoxSharedLinkRequest;
import java.io.FileReader;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.core.DateState;

import java.io.Reader;
public class SharedLink8b {
   
   
    //private static String PRIMARY_ACCESS_TOKEN = "4WiptPRjD9mxEZb6Ngq3geJejjRSRwOG";
    //private static String SHARED_FOLDER_LINK = "https://ucop.box.com/s/yohjgd5w6t0t3v8oktcogl5edsdg836d";
    // WORKS! private static String SHARED_FOLDER_LINK = "https://ucop.app.box.com/folder/200070680194?s=yohjgd5w6t0t3v8oktcogl5edsdg836d";
    //private static String SHARED_FOLDER_LINK = "https://ucop.box.com/s/euvvhaopsb7q1gckmy79d91h8mige2xf";
    //private static String SHARED_FOLDER_LINK = "https://ucop.box.com/v/thisispubtest";
    //private static String SHARED_FOLDER_LINK = "https://ucop.app.box.com/v/ucnonprivate";
    //private static String SHARED_FOLDER_LINK = "https://ucop.box.com/s/a717dr53ersytrs3sqgqoq58tnis6fzy";
    private static String SHARED_FOLDER_LINK = "https://ucop.app.box.com/folder/202940033692?s=a717dr53ersytrs3sqgqoq58tnis6fzy";
    private static String CONFPATH = "/home/loy/tasks/box/230310-extract/2384924_afe65ks1_config.json";
    private static String ACCESS = "bSZXPImYloU7G8mxxxxxxxxxxx";
    public static void main(String args[])
    {
        BoxFile.Info boxFile = null;
        String path = "";
        LoggerInf logger = new TFileLogger("rebuild", 10, 10);
        try {
           
            Reader reader = new FileReader(CONFPATH);
            BoxConfig boxConfig = BoxConfig.readFrom(reader);
            System.out.println(boxConfig.toString());
            BoxDeveloperEditionAPIConnection apiJWT = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
            BoxAPIConnection apiPrivate = new BoxAPIConnection(ACCESS);
           
            BoxAPIConnection api = apiPrivate; //apiJWT;
            BoxItem.Info boxItem = BoxFolder.getSharedItem(api, SHARED_FOLDER_LINK);
            BoxFolder publicFolder = (BoxFolder)boxItem.getResource();
            Iterable<com.box.sdk.BoxItem.Info> items = publicFolder.getChildren();
            for (BoxItem.Info item : items) {
                System.out.println("\t" + item.getName() + " - sharedLink:" + item.getSharedLink());
                dump(item);
                processItem(api, item);
            }
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    } 
    
    protected static void dump(BoxItem.Info item)
       throws Exception
    {
        DateState creation = new DateState(item.getCreatedAt());
        System.out.println("boxItem:" + item.getName() + "\n"
                + " - size=" + item.getSize() + "\n"
                + " - created=" + creation.getIsoDate() + "\n"
        );
    }
    
    
    public static void processItem(BoxAPIConnection api, BoxItem.Info itemInfo)
        throws Exception
    {
       
        try {
            if (itemInfo instanceof BoxFile.Info) { 
                String fileID = itemInfo.getID();
                System.out.println("fileID=" + fileID);
                BoxFile.Info fileInfo = (BoxFile.Info) itemInfo;
                dumpItemInfo("file", fileInfo);
                BoxFile apiFile = new BoxFile(api, fileID);
                BoxFile.Info apiInfo = apiFile.getInfo();
                dumpItemInfo("api", apiInfo);
                BoxFile sharedFile = (BoxFile)itemInfo.getResource();
                BoxFile.Info resourceInfo = sharedFile.getInfo();
                System.out.println("shared preview" + sharedFile.getPreviewLink());
                dumpItemInfo("resource", resourceInfo);
                dumpItem("apiFile", apiFile);
                dumpItem("sharedFile", sharedFile);
                
                

            } else if (itemInfo instanceof BoxFolder.Info) {
                System.out.println("Folder:" + itemInfo.getName());
                /*
                String localPath = path;
                BoxFolder.Info folderInfo = (BoxFolder.Info) itemInfo;
                localPath = localPath + itemInfo.getName();
                File dir = new File(localPath);
                if (!dir.exists() ) {
                    System.out.println("create dir:" + localPath);
                    Files.createDirectory(Paths.get(localPath));
                }
                localPath = localPath + "/";
                String folderId = folderInfo.getID();
                System.out.println("dump folder - "
                        + " - folderId:" + folderId
                        + " - localPath:" + localPath
                );
                processItem(api, folderId, localPath, logger);
                */
            }
                       
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    public static void dumpItemInfo(String header, BoxItem.Info itemInfo)
        throws Exception
    {
       
        try {
            System.out.println("=== ItemInfo " + header + "===");
            if (itemInfo instanceof BoxFile.Info) {
                BoxFile.Info fileInfo = (BoxFile.Info)itemInfo;
                System.out.println("ID:" + fileInfo.getID());
                System.out.println("name:" + fileInfo.getName());
                System.out.println("version:" + fileInfo.getVersionNumber());
                System.out.println("Sha1:" + fileInfo.getSha1());
                System.out.println("Size:" + fileInfo.getSize());
                System.out.println("Created:" + fileInfo.getCreatedAt());
                System.out.println("preview link:" + fileInfo.getPreviewLink());

            } else if (itemInfo instanceof BoxFolder.Info) {
                System.out.println("Folder:" + itemInfo.getName());
                /*
                String localPath = path;
                BoxFolder.Info folderInfo = (BoxFolder.Info) itemInfo;
                localPath = localPath + itemInfo.getName();
                File dir = new File(localPath);
                if (!dir.exists() ) {
                    System.out.println("create dir:" + localPath);
                    Files.createDirectory(Paths.get(localPath));
                }
                localPath = localPath + "/";
                String folderId = folderInfo.getID();
                System.out.println("dump folder - "
                        + " - folderId:" + folderId
                        + " - localPath:" + localPath
                );
                processItem(api, folderId, localPath, logger);
                */
            }
                       
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    public static void dumpItem(String header, BoxItem  itemInfo)
        throws Exception
    {
       
        try {
            String metadataS = "none";
            String downloadS = "none";
            Metadata metadata = null;
            URL download = null;
            System.out.println("=== BoxFile " + header + "===");
            if (itemInfo instanceof BoxFile) {
                BoxFile boxFile = (BoxFile)itemInfo;
                try {
                    metadata = boxFile.getMetadata();
                    metadataS = metadata.toString();
                } catch (Exception ex) {
                    metadataS = ex.toString();
                }
                try {
                    download = boxFile.getDownloadURL();
                    downloadS = download.toString();
                } catch (Exception ex) {
                    downloadS = ex.toString();
                }
                System.out.println("ID:" + boxFile.getID());
                System.out.println("preview link:" + boxFile.getPreviewLink());
                System.out.println("metadata:" + metadataS);
                System.out.println("download:" + downloadS);

            } else {
                
            }
                       
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    }
       
    private static BoxSharedLink createSharedLink(BoxAPIConnection api, String fileId)
        throws Exception
    {
        try {
  
            BoxFile file = new BoxFile(api, fileId);
            BoxSharedLink.Permissions permissions = new BoxSharedLink.Permissions();
            permissions.setCanDownload(true);
            permissions.setCanPreview(true);
            Date date = new Date();

            Calendar unshareAt = Calendar.getInstance();
            unshareAt.setTime(date);
            unshareAt.add(Calendar.DATE, 14);
            
            BoxSharedLinkRequest sharedLinkRequest = new BoxSharedLinkRequest()
                .access(BoxSharedLink.Access.OPEN)
                .permissions(true, true);
            sharedLinkRequest.unsharedDate(unshareAt.getTime());
            
            BoxSharedLink sharedLink = file.createSharedLink(sharedLinkRequest);
            System.out.println("shared link: " + sharedLink.getURL());
            return  sharedLink;
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            throw ex;
            
        }
    }
    
    
       
    private static Metadata createMetadata(BoxAPIConnection api, String fileId)
        throws Exception
    {
        try {
            String indent = "   ";
            BoxFile file = new BoxFile(api, fileId);
            Metadata metadata = file.getMetadata();
            System.out.println(indent + "metadata: " + metadata.toString());
            return metadata;
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            throw ex;
            
        }
    }
}
