package LASER;

import LASER.Utils.HDFSUtil;

import java.io.IOException;

public class App 
{
    public static void main( String[] args ) throws IOException
    {
        HDFSUtil.cleanupTemporaryPath();
    }
}
