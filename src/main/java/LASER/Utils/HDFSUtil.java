package LASER.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.tools.tree.NewArrayExpression;

import java.io.IOException;

public class HDFSUtil {

    private static final String LASER_PATH = "Laser";

    private static Path getLaserPath(){
        return new Path(LASER_PATH);
    }

    public static Path getInputPath(){
        return new Path(getLaserPath(), "input");
    }

    public static Path getOutputPath(){
        return new Path(getLaserPath(), "output");
    }

    public static Path getTemporaryPath(){
        return new Path(getLaserPath(),"temp");
    }

    public static boolean cleanupTemporaryPath() throws IOException{
        FileSystem hdfs = FileSystem.get(new Configuration());

        Class callingClass = new Throwable().getStackTrace()[2].getClass();
        Logger logger = LoggerFactory.getLogger(callingClass);

        boolean deleteSucceeded = hdfs.delete(getTemporaryPath(), true);

        if(deleteSucceeded) {
            logger.info("Remaining temporary files successfully removed.");
        } else {
            logger.warn("Could not remove temporary files. They might have already been removed in a previous run. " + getTemporaryPath().toString());
        }

        return deleteSucceeded;
    }
}
