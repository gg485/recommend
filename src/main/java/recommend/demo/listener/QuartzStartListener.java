package recommend.demo.listener;

import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import recommend.demo.config.HiveQuartzScheduler;

@Component
public class QuartzStartListener implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired
    HiveQuartzScheduler scheduler;
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            scheduler.startJob();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
