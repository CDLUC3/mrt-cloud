/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test;
import org.cdlib.mrt.s3.tools.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.Properties;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
import org.cdlib.mrt.s3.tools.CloudChecksum;
/**
 *
 * @author replic
 */
public class ChecksumStat {
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "md5",
                "sha256"
            };
    protected static String [] jarVersions = {
                "nodes-remote_v1",
                "nodes-remote_v2"
            };
    protected ArrayList<ChecksumStatEntry> statArr = new ArrayList<>();
    
    protected ArrayList<VersionTotal> versionTotals = new ArrayList<>();
    
    
    public ChecksumStat(int vcnt, LoggerInf logger) 
            throws TException
    {
        this.logger = logger;
        for (int i=0; i<vcnt; i++) {
            VersionTotal entry = new VersionTotal();
            versionTotals.add(entry);
        }
    }
    
    public void buildStat()
    {
        long totalTime = 0;
            for (ChecksumStatEntry stat : statArr) {
                VersionTotal versionTotal = versionTotals.get(stat.version - 1);
                versionTotal.vbytes += stat.len;
                versionTotal.vcnt++;
                versionTotal.vtime += stat.runTime;
                totalTime += stat.runTime;
            }
            for (int i=0; i<versionTotals.size(); i++) {
                VersionTotal versionTotal = versionTotals.get(i);
                int version = i+1;
                double averageVersionTime = (double) versionTotal.vtime / versionTotal.vcnt;
                double averageTotalTime = (double) versionTotal.vtime / (double)totalTime;
                System.out.println("Version(" + version + "):" 
                        + " - count=" + versionTotal.vcnt
                        + " - time=" + versionTotal.vtime
                        + " - averageVersionTime=" + averageVersionTime
                        + " - averageTotalTime=" + averageTotalTime
                );
            }
    }
    
    public void addEntry(
            int version,
            long node,
            long runTime,
            long len,
            String key)
    {
        
            ChecksumStatEntry runStat = new ChecksumStatEntry(version, node, runTime, len, key);
            statArr.add(runStat);
            
    }
    
    public void dumpEntry()
    {
        
            for (ChecksumStatEntry stat : statArr) {
                stat.dump();
            }
            
    }
    private static class VersionTotal {
        public long vcnt = 0;
        public long vbytes = 0;
        public long vtime = 0;
        public VersionTotal() { }
    }
    
    private static class ChecksumStatEntry {
        public long node = 0;
        public int version = 0;
        public long runTime = 0;
        public String key = "";
        public long len = 0;
        
        public ChecksumStatEntry(
            int version,
            long node,
            long runTime,
            long len,
            String key)
        {
                this.node = node;
                this.version = version;
                this.runTime = runTime;
                this.len = len;
                this.key = key;
        }
        
        public void dump()
        {
            String msg = 
                    "ChecksumStatEntry:"
                    + " - version:" + version
                    + " - node:" + node
                    + " - runTime:" + runTime
                    + " - len:" + len
                    + " - key:" + key;
            System.out.println(msg);
        }
    }
}
