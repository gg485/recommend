package recommend.demo.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;

public class ToHiveJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {Process p = Runtime.getRuntime().exec(
                "cmd /c " +
                        "hive -e " +
                        "\"drop table if exists ratings;" +
                        "create table ratings(userid int,movieid int,rating double) row format delimited fields terminated by ',';\"");
            p.waitFor();
            p = Runtime.getRuntime().exec(
                    "cmd /c " +
                            "sqoop import --connect \"jdbc:mysql://localhost:3306/recommend?serverTimezone=UTC\" " +
                            "--username root --password gddsygy_12345 --table alstab " +
                            "--num-mappers 1 --hive-import --fields-terminated-by \",\" --hive-overwrite --hive-table ratings");
            p.waitFor();
            p.destroy();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
