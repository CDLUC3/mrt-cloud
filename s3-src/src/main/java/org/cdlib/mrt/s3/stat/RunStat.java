/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.stat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.cdlib.mrt.utility.*;
/**
 *
 * @author replic
 */
public class RunStat {
    
    protected LoggerInf logger = null;
    protected String isoDate = null;
    
    HashMap<String, ArrayList> categories = new HashMap<>();
    HashMap<String, TallyEntry> tallies = new HashMap<>();
    
    
    public RunStat(LoggerInf logger) 
            throws TException
    {
        this.logger = logger;
        isoDate = DateUtil.getCurrentIsoDate();
    }
    
    public void addTallyEntries()
        throws TException
    {
        Set<String> keys = categories.keySet();
        for (String key : keys) {
            addTallyEntry(key);
        }
    }
    
    public void addTallyEntry(String category)
        throws TException
    {
        ArrayList<StatEntry> categoryList = categories.get(category);
        if (categoryList == null) {
            throw new TException.INVALID_OR_MISSING_PARM("buildTallyEntry missing:" + category);
        }
        TallyEntry tallyEntry = tallies.get(category);
        if (tallyEntry == null) {
            tallyEntry = new TallyEntry(category, isoDate);
            tallies.put(category, tallyEntry);
        }
        
        
        for (StatEntry categoryEntry : categoryList) {
            tallyEntry.addTallyEntry(categoryEntry);
        }
    }
    
    public void dumpTallyEntry(String category)
        throws TException
    {
        TallyEntry tallyEntry = tallies.get(category);
        if (tallyEntry == null) {
            throw new TException.INVALID_OR_MISSING_PARM("dumpTallyEntry missing:" + category);
        }
        System.out.println(tallyEntry.dump());
    }
    
    public void addEntry(
            String category,
            int cnt,
            long runTime,
            long len,
            boolean match,
            String note)
    {
        ArrayList<StatEntry> categoryList = categories.get(category);
        if (categoryList == null) {
            categoryList = new ArrayList<>();
            categories.put(category, categoryList);
        }
        StatEntry runStat = new StatEntry(category, cnt, runTime, len, match, note);
        categoryList.add(runStat);
            
    }
    
    public void dumpEntries(String category)
        throws TException
    {
        ArrayList<StatEntry> categoryList = categories.get(category);
        if (categoryList == null) {
            throw new TException.INVALID_OR_MISSING_PARM("dumpEntries category missing:" + category);
        }
        for (StatEntry stat : categoryList) {
            stat.dump();
        }
    }
    
    private static class TallyEntry {
        public String category = "";
        public int count = 0;
        public long runTime = 0;
        public long size = 0;
        public int minCount = 10000000;
        public long minRunTime = 1000000000000L;
        public long minSize = 1000000000000L;
        public int maxCount = -1;
        public long maxRunTime = -1;
        public long maxSize = -1;
        public int matchOKCnt = 0;
        public int matchFailCnt = 0;
        
        
        
        public TallyEntry(String category, String isoDate) {
            this.category = category;
            this.isoDate = isoDate;
        }
        
        public String isoDate = null;
        
        public void addTallyEntry(StatEntry statEntry)
        {
            category = statEntry.category;
            count += statEntry.count;
            runTime += statEntry.runTime;
            size += statEntry.size;
            if (statEntry.count < minCount) minCount = statEntry.count;
            if (statEntry.count > maxCount) maxCount = statEntry.count;
            if (statEntry.runTime < minRunTime) minRunTime = statEntry.runTime;
            if (statEntry.runTime > maxRunTime) maxRunTime = statEntry.runTime;
            if (statEntry.size < minSize) minSize = statEntry.size;
            if (statEntry.size > maxSize) maxSize = statEntry.size;
            if (statEntry.match) matchOKCnt++;
            else matchFailCnt++;
            
        }
        
        public static double diff(long num, long denom)
        {
            return (double) num / (double) denom;
        }
        
        public double diffCount()
        {
            return diff(maxCount, minCount);
        }
        
        public double diffRunTime()
        {
            return diff(maxRunTime, minRunTime);
        }
        
        public double diffSize()
        {
            return diff(maxSize, minSize);
        }
        
        public double avgRunTime()
        {
            return diff(runTime, count);
        }
        
        public String dump() {
            String out =
                    category + " : " +  isoDate + "\n"
                 + " - matchOKCnt=" + matchOKCnt  + " - matchFailCnt=" + matchFailCnt + "\n" 
                 + " - count=" + count + " - minCount=" + minCount + " - maxCount=" + maxCount + " - diffCount=" + diffCount() + "\n"
                 + " - runTime=" + runTime + " - avgRunTime=" + avgRunTime() + " - minRunTime=" + minRunTime + " - maxRunTime=" + maxRunTime + " - diffRunTime=" + diffRunTime() + "\n"
                 + " - size=" + size + " - minSize=" + minSize + " - maxSize=" + maxSize + " - diffSize=" + diffSize() + "\n"
                 ;
            return out;
        }
        
    }
    
    private static class StatEntry {
        public String category = "";
        public int count = 0;
        public long runTime = 0;
        public long size = 0;
        public Boolean match = null;
        public String note = "";
        
        public StatEntry(
            String category,
            int count,
            long runTime,
            long size,
            Boolean match,
            String note)
        {
            this.category = category;
            this.count = count;
            this.runTime = runTime;
            this.size = size;
            this.note = note;
            this.match = match;
        }
        
        public void dump()
        {
            double lenTime = (double)size/(double)runTime;
            double timeCnt = (double)runTime/(double)count;
            String msg = 
                    "ChecksumStatEntry:"
                    + " - category:" + category
                    + " - count:" + count
                    + " - runTime:" + runTime
                    + " - size:" + size
                    + " - match:" + match
                    + " - note:" + note
                    + " - lenTime:" + getLenTime()
                    + " - timeCnt:" + getTimeCnt();
            System.out.println(msg);
        }
        
        public double getLenTime()
        {
            return (double)size/(double)runTime;
        }
        
        public double getTimeCnt()
        {
            return (double)runTime/(double)count;
        }
    }
}
