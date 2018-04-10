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
package org.cdlib.mrt.s3.pairtree;

import java.io.File;
import java.util.Vector;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author David Loy
 */
public class PairtreeUtilLocal 
{
    protected static final String NAME = "PairtreeUtilLocal";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    
    /**
     * Build if necessary directories down to object level
     * @param baseDirectory base directory level for file generation
     * @param name name used for pair tree construction
     * @return named directory file
     */ 
    public static File buildPairDirectory(
            File baseDirectory,
            String name)
        throws TException
    {
        if ((baseDirectory == null) || !baseDirectory.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "baseDirectory missing");
        }
        if (StringUtil.isEmpty(name)) return null;
        try {
            Vector<String> lvls = getPairLevels(name);
            File builtDir = addDirectoryLevels(baseDirectory, lvls);
            if ((builtDir == null) || !builtDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "moveDirectory missing");
            }
            //System.out.println("getObjectDirectory:" + moveDir.getAbsolutePath());
            return builtDir;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "getObjectDirectory - Exception:" + ex);
            return null;
        }
    }

    /**
     * Return a file that may not be resolved for pair path
     * @param baseDirectory base directory level for file generation
     * @param name name used for pair tree construction
     * @return named directory file
     */
    public static File getPairDirectory(
            File baseDirectory,
            String name)
        throws TException
    {
        if (baseDirectory == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "baseDirectory missing");
        }
        if (!baseDirectory.exists()) {
            baseDirectory.mkdir();
        }
        if (StringUtil.isEmpty(name)) return null;
        try {
            Vector<String> lvls = getPairLevels(name);
            File pathDir = getPairPath(baseDirectory, lvls);
            return pathDir;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "getObjectDirectory - Exception:" + ex);
            return null;
        }
    }

    /**
     * Remove pair directory and remove a parent directory to baseDirectory, but not
     * including base directory
     * @param deleteDirectory directory to be deleted
     * @param name name used for pair tree construction
     * @return true=all pairtree leaves removed; false= some directory level not empty
     */
    public static boolean removePairDirectory(
            File deleteDirectory)
        throws TException
    {
        if ((deleteDirectory == null) || !deleteDirectory.exists()) {
            return true;
        }
        try {
            if (DEBUG) System.out.println(MESSAGE + "removePairDirectory:"
                    + " - deleteDirectory=" + deleteDirectory.getAbsolutePath()
                    );
            File parentFile = deleteDirectory.getParentFile();
            FileUtil.deleteDir(deleteDirectory);
            FileUtil.deleteEmptyPath(parentFile);
            return true;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "removePairDirectory - Exception:" + ex);
            return false;
        }
    }

    /**
     * Build if necessary directories down to object level
     * @param baseDirectory base directory level for file generation
     * @param lvls list of intermediate directory levels to be created
     * @return named directory file
     */
    public static File addDirectoryLevels(
            File baseDirectory,
            Vector<String> lvls)
    {
        if (baseDirectory == null) return null;
        if ((lvls == null) || (lvls.size() == 0)) return null;
        boolean success = false;
        try {
            if (!baseDirectory.exists()) {
                success = baseDirectory.mkdirs();
                if (!success) return null;
            }
            File moveDir = baseDirectory;
            for (int i=0; i<lvls.size(); i++) {
                String lvldir = lvls.get(i);
                moveDir = new File(moveDir, lvldir);
                if (!moveDir.exists()) {
                    success = moveDir.mkdirs();
                    if (!success) return null;
                }
            }
            if (DEBUG) System.out.println("getObjectDirectory:" + moveDir.getAbsolutePath());
            return moveDir;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "getObjectDirectory - Exception:" + ex);
            return null;
        }
    }

    /**
     * Build if necessary directories down to object level
     * @param baseDirectory base directory level for file generation
     * @param lvls list of intermediate directory levels to be created
     * @return named directory file
     */
    public static File getPairPath(
            File baseDirectory,
            Vector<String> lvls)
    {
        if (baseDirectory == null) return null;
        if ((lvls == null) || (lvls.size() == 0)) return null;
        try {
            File moveDir = baseDirectory;
            for (int i=0; i<lvls.size(); i++) {
                String lvldir = lvls.get(i);
                moveDir = new File(moveDir, lvldir);
            }
            if (DEBUG) System.out.println("getObjectDirectory:" + moveDir.getAbsolutePath());
            return moveDir;

        } catch (Exception ex) {
            System.out.println(MESSAGE + "getObjectDirectory - Exception:" + ex);
            return null;
        }
    }

    /**
     * Delete a constructed pair path if path is empty
     * @param baseDirectory begin pair path here
     * @param lvls String levels for constructing path
     * @return
     */
    protected static boolean deletePairPath(
            File baseDirectory,
            Vector<String> lvls)
        throws TException
    {
        if (baseDirectory == null) return true;
        if ((lvls == null) || (lvls.size() == 0)) return true;
        Vector<File> files = new Vector<File>(lvls.size());
        try {
            File file = baseDirectory;
            for (int i=0; i<lvls.size(); i++) {
                String lvldir = lvls.get(i);
                file = new File(file, lvldir);
                files.add(file);
            }
            for (int i = (lvls.size()-1); i >= 0; i--) {
                file = files.get(i);
                if ((file != null) && file.exists()) {
                    boolean deleted = file.delete();
                    if (!deleted) return false; // contains other nodes
                }
            }
            return true;

        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION("Exception:" + ex);
        }
    }

    /**
     * Get a List of String pairtree elements
     * @param name Name used for pairtree list generation
     * @return pairtree list
     */
    protected static Vector<String> getPairLevels(String name)
        throws TException
    {
        name = getPairName(name);

        Vector<String> lvls = new Vector(20);
        String seg = name;
        for (int i=0; i < seg.length(); i += 2) {
            int len = seg.length() - i == 1 ? 1 : 2;
            String lvldir = seg.substring(i, i + len);
            if (DEBUG) System.out.println("getLvls i=" + i + " - len=" + len + " - lvldir=" + lvldir);
            lvls.add(lvldir);
        }
        lvls.add(name);
        return lvls;
    }



    /**
     * Get a pairtree path based on input name
     * @param name
     * @return pairtree path as String
     */
    public static String getPairName(String name)
        throws TException
    {
        if (StringUtil.isEmpty(name)) return "";
        byte[] barr = new byte[1];
        try {
            StringBuffer buf = new StringBuffer();
            byte[] bytes = name.getBytes("utf-8");
            byte b = 0;
            for (int i=0; i<bytes.length; i++) {
                b = bytes[i];
                int ib = b & 0xff;
                if (ib < 0x21) buf.append(hex(ib));
                else if (ib > 0x7e) buf.append(hex(ib));
                else {
                    int pos = "\\\"*+,<=>?^|".indexOf(ib);
                    if (pos >= 0) buf.append(hex(ib));
                    else {
                        barr[0] = b;
                        String s = new String(barr);
                        buf.append(s);
                    }
                }
            }
            if (buf.length() == 0) return "";
            name = buf.toString();
            name = name.replace('/', '=');
            //name = name.replace('\\', '=');
            name = name.replace(':', '+');
            name = name.replace('.', ',');
            return name;

        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(
                    MESSAGE + "getPairNameUpdate - Exception:" + ex);
        }
    }

    public static String hex(int ib)
        throws Exception
    {
        String hexstr = Integer.toString(ib, 16);
        if (hexstr.length() == 1) hexstr = "0" + hexstr;
        return "^" + hexstr;
    }
    
}
