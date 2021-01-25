Modified in JasonWang Branch

This is Jason Wang's home work for Algo-ETL

How to run this process:
  The process can be scheduled to run in AWS:EC2:Linux using crontab.Here is the setup for crontab.
      SHELL=/bin/bash
      30 20 * * 1-5 /home/ubuntu/DS_project/finnhub_etl.sh > /tmp/jwrun.log 

This job is rerunable, if the job fails, simply rerun the /home/ubuntu/DS_project/finnhub_etl.sh or reschedule to run in crontab



