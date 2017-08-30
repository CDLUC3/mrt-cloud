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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Properties;

import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Driver routine for BuildObjectManifest. Not generalized!
 */
public class RebuildWithFiles {
    
    protected static final String NAME = "RebuildWithFiles";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected AddObjectComponent aoc = null;
    protected LoggerInf logger = null;
    
    public RebuildWithFiles(AddObjectComponent aoc, LoggerInf logger)
        throws TException
    {
        this.aoc = aoc;
        this.logger = logger;
    }
    
    public static void main(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        main1(args);
    }
    
    
    
    public static void main1(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        System.out.println("Run main1");
        TFrame tFrame = null;
        File component = null;
        try {
            String propertyList[] = {
                "resources/" + NAME + ".properties"};
            tFrame = new TFrame(propertyList, NAME);
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("rebuild", 10, 10);
            OpenstackCloud cloud = OpenstackCloud.getOpenstackCloud(runProp, logger);
            AddObjectComponent aoc = AddObjectComponent.getAddObjectComponent(
                cloud,
                "prod-9103",
                logger);
            RebuildWithFiles rwf = new RebuildWithFiles(aoc, logger);
            
            /*
            if (false) rwf.addComponent(
                    "/replic/loy/sdsc-recover/components/13030-kt609nb3jq^7c1^7carchival,tif/component.txt",
                    "ark:/13030/kt609nb3jq",
                    1,
                    "producer/archival.tif",
                    "a31c98542a724aeec4547669ee745b9a90b3720e580d95cc86a961a9cd49f06b",
                    18142250);
            
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/13030-kt109nd8mh^7c1^7cFILEID-1,74,36,gif/component.txt",
                "ark:/13030/kt109nd8mh",
                1,
                "producer/FILEID-1.74.36.gif",
                "629755e5f2ef0c683bde22652c9097900b01415048890f938857930c24e88327",
                22963);


            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/13030-kt6w1023hp^7c1^7cthumbnail,gif/component.txt",
                "ark:/13030/kt6w1023hp",
                1,
                "producer/thumbnail.gif",
                "a5f7b0b59f7aa506612e5cf1a41dceddaaf98237afc2eca442bfec63d6903a5b",
                27036);


            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/13030-ft5d5nb3cg^7c1^7cfigures=ft5d5nb3cg_00298,gif/component.txt",
                "ark:/13030/ft5d5nb3cg",
                1,
                "producer/figures/ft5d5nb3cg_00298.gif",
                "d4d133928f3ecf9f79b48dcdc0e3fc0464915f62b378b8ddfa0b5b0d00bb5abd",
                1167);


            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/21198-zz0001rqcf^7c1^7cFID1-VI_81656-A_BVE42183-2,wav/component.txt",
                "ark:/21198/zz0001rqcf",
                1,
                "producer/FID1-VI_81656-A_BVE42183-2.wav",
                "c65f729cbe310ea7ec5afded51bc145872abebf55b690c6914cfa5678a2271db",
                56281320);
                * 
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/13030-vf2q52h9x^7c5^7ccdlmeta,tar,gz/component.txt",
                "ark:/13030/vf2q52h9x",
                5,
                "producer/cdlmeta.tar.gz",
                "01100d1564eb89661c8089ff6a28bbaef50eed3e2c2d0235f369c5186f4de7e6",
                16804);
                
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/21198-zz0001vf1r/content.txt",
                "ark:/21198/zz0001vf1r",
                1,
                "producer/mrt-dc.xml",
                "6273b819cb0477b687d08d6aa5521d3cabc61dfe1b266d7dfcededfd1510e578",
                1003);
                * 
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/28722-bk0005c5637/content.txt",
                "ark:/28722/bk0005c5637",
                1,
                "producer/mrt-erc.txt",
                "90d394c9966554f4e4f35df324e11f69217b9547691ca96c47bfd76cf76fe7e4",
                552);        
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/13030-vf0ks6ks2/content.txt",
                "ark:/13030/vf0ks6ks2",
                1,
                "system/mrt-erc.txt",
                "539bd64b6b366000c128fa2159a58be7845923f394805ba2e26c79f9cbf46e15",
                190);        
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/21198-zz00005btt/content.txt",
                "ark:/21198/zz00005btt",
                1,
                "system/mrt-erc.txt",
                "535d6a0c5c412ef11b939047469bcc6b1667f04f4a009a7af57dc106ab5ff8de",
                186);
                        
            if (false) rwf.addComponent(
                "/replic/loy/sdsc-recover/components/28722-k24x56j2p/content.txt",
                "ark:/28722/k24x56j2p",
                1,
                "system/mrt-erc.txt",
                "1fdf51350022e37f55c34aa852494ac8ec1b79988bcb5feff9af026b52b4631e",
                253);
            */

            

            
            if (false) rwf.addComponent(
                true,
                "/replic/tasks/141023-SDSC-recover/content/k2n876h61/mrt-owner.txt",
                "ark:/28722/k2n876h61",
                1,
                "system/mrt-owner.txt",
                "745f5b94951bb93dbe0a793cc0401980bb4dd8bbb372ffee8cefee4dc33707ee",
                19);  
            
            if (false) rwf.addComponent(
                true,
                "/replic/tasks/141023-SDSC-recover/content/k2dn43d7d/mrt-dc.xml",
                "ark:/28722/k2dn43d7d",
                1,
                "system/mrt-dc.xml",
                "f40dd72e54b7e93c389895de1c13922fe5ac2ac226d7159a9704ae6f19a67929",
                149);  
            
            if (false) rwf.addComponent(
                false,
                "/replic/tasks/141023-SDSC-recover/content/k2ww7bh7j/UBERON_0006849",
                "ark:/28722/k2ww7bh7j",
                1,
                "producer/purl.obolibrary.org/obo/UBERON_0006849",
                "f0ed38a844983a8148a19ac46ec718c552b7c6e0c39e7662dea0fa51a5b95587",
                39026);
            
            if (false) rwf.addComponent(
                true,
                "/replic/tasks/141023-SDSC-recover/content/m54f1xq2/mrt-erc.txt",
                "ark:/13030/m54f1xq2",
                1,
                "system/mrt-erc.txt",
                "e6cfc4c1a33c61018b9fbc89e8fd5c5b980b7bc0ab20ac2a6c9731242da80a6f",
                133);
            
            if (true) rwf.addComponent(
                true,
                "/replic/tasks/141023-SDSC-recover/content/m5c82wrx/manifest.xml",
                "ark:/13030/m5c82wrx|manifest",
                "6a296c4cac198a8d5cdfd5c7a9eea00710c8f0677c592af2e59bdb70ed0c0568",
                5338);

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
    }
    
    public void addComponent(
            boolean doAdd,
            String fileS,
            String objectIDS,
            int version,
            String fileID,
            String checksum,
            long size)
        throws TException
    {
        try {
            String checksumType = "sha-256";
            File component = new File(fileS);
            if (!component.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "file missing");
            }
            FixityTests fixity = new FixityTests(component, checksumType, logger);
            FixityTests.FixityResult result = fixity.validateSizeChecksum(checksum, checksumType, size);
            if (!(result.checksumMatch && result.fileSizeMatch)) {
                throw new TException.FIXITY_CHECK_FAILS(component.getCanonicalPath());
            }
            //aoc.addComponent(objectIDS, version, fileID, component, true, true);
            aoc.addComponent(objectIDS, version, fileID, component, doAdd, true);
                    
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException (ex);
        }
    }
    
    public void addComponent(
            boolean doAdd,
            String fileS,
            String key,
            String checksum,
            long size)
        throws TException
    {
        try {
            String checksumType = "sha-256";
            File component = new File(fileS);
            if (!component.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "file missing");
            }
            FixityTests fixity = new FixityTests(component, checksumType, logger);
            FixityTests.FixityResult result = fixity.validateSizeChecksum(checksum, checksumType, size);
            if (!(result.checksumMatch && result.fileSizeMatch)) {
                throw new TException.FIXITY_CHECK_FAILS(component.getCanonicalPath());
            }
            //aoc.addComponent(objectIDS, version, fileID, component, true, true);
            aoc.addComponent(key, component, doAdd, true);
                    
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException (ex);
        }
    }
}
