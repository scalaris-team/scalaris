package de.zib.scalaris.examples.wikipedia.data.xml.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

/**
 * Provides an InputStream interface to a SevenZFile  
 */

public class SevenZInputStream extends ArchiveInputStream {

    private SevenZFile archive;
    private SevenZArchiveEntry currentEntry;
    
    public SevenZInputStream(File file) throws IOException {
        archive = new SevenZFile(file);
        currentEntry = archive.getNextEntry();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(currentEntry != null) {
            return archive.read(b, off, len);
        } else {
            return -1;
        }
    }

    public SevenZArchiveEntry getCurrentEntry() {
        return currentEntry;
    }
    
    @Override
    public SevenZArchiveEntry getNextEntry() throws IOException {
        currentEntry = archive.getNextEntry();
        return currentEntry;
    }
    
    @Override
    public void close() throws IOException {
        currentEntry = null;
        archive.close();        
    }

}
