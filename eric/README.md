# DS-Algo Assignment#1 - ETL 



## Tasks 

- Create Python script in AWS EC2 to fetch data from finnhub, and load it to AWS RDS:
  - You can choose any data;
  -  You can use any of these libs: request, finnhub-python, petl, pandas, etc. 

-  Create shell script to run above job automatically and recursively: 
  - You can use any job scheduling tools, prefer use crontab 
- Implement simple alter/notification system:
  -  Notify ETL job finished status, either via email or SMS
  -  Sending Alert during etl job if anything need immediate attention



## Mark 

- Satisfactory (60%)
   - 15-Data was loaded from finnhub to AWS RDS;
   - 10-Job was scheduled and runs well;
   - 10-Notification / Alert works as expected;
   - 10-Applied basic best practice;
   - 10-Clear project structure;
   -  5-DB credential well managed;
- Above and beyond (40%) 
   - 10-Exceptions handled properly;
   - 10-Small jobs running parallel/dependently; 
   - 10-Applied most best practices;
   -  5-Multiple solutions for loading, eg, use API and lib;
   -  5-Dual channels(Email, SMS) for notification/alert;
- Nice to Have(20%)：
    - 5-Configurable ETL behaviour;
    - 5-Considered performance;
    - 5-Unit test available;
    - 5-All ETL jobs are idempotent;



- Cap: 100%



## Submit process

- **DB** : Create a user in your RDS with read access to all your tables

  - User name: algo

  - password: DS-Algo-ETL

    

- **Code** : Fork repo  from github.com/BinYuOnCa/Algo-ETL and add your code

  - Create a new branch, name it as your name in WeChat group, use pinyin if needed

  - Add all you code into the branch, and follow best practice to submit/push

  - File a pull request when you are done

  - You can push code anytime, but only those filed before the due date will be accepted

    

- **Release:** All pull requests will be approved as is after we finish the assessment, so that you can share/learn from each other.

  