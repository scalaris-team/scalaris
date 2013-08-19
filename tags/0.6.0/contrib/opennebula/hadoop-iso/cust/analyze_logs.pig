register /root/piggybank.jar;
DEFINE LogLoader org.apache.pig.piggybank.storage.apachelog.ContrailLogLoader();
DEFINE MyDateExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd:HH:mm:ss');

logs = LOAD '$LOGS' USING LogLoader as (remoteAddr, remoteLogname, user, time, method, uri, title, proto);
logs = FOREACH logs GENERATE MyDateExtractor(time) as date, title;
logs = FILTER logs BY NOT (title matches '[^:]+:\\S+');

-- get the 10 most popular sites
site_groups = GROUP logs BY title;
by_site = FOREACH site_groups GENERATE group, COUNT(logs.date) AS cnt;
by_site = ORDER by_site BY cnt DESC;
by_site = LIMIT by_site 10;

-- get hits per minute
date_groups = GROUP logs BY date;
by_date = FOREACH date_groups GENERATE group, COUNT(logs.date) AS cnt;
-- log files are already ordered by date

STORE by_site INTO 'pigout/by_site';
STORE by_date INTO 'pigout/by_date';

