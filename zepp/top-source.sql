/*
 Get users by Top Source
 */

%sql 
select clickLog.tRef as source, count(articleRead.id)  as Total_Views from articleRead , clickLog where articleRead.id = clickLog.id group by clickLog.tRef
HAVING Total_Views >= 30000 order by Total_Views DESC 


/*
 Overal Querry Run
 */

%sql 
select tSession as Session, sum(1) as total_views, sum(iFb) as fb, sum(iDir) as direct, sum(iInt) as internal, sum(iExt) as external from 
clickLog group by tSession having total_views > 1


/**
 * Time Classifier
 */
%sql
select classifier, sum(1)*100/sum(count(*)) over () as percent_views from clickLog group by classifier


/**
 * All traffic from www.facebook.com
 */

%sql 
select clickLog.tRef as source, count(articleRead.id)  as Total_Views from articleRead , clickLog where articleRead.id = clickLog.id and 
clickLog.tRef like "%facebook.%"  group by clickLog.tRef order by Total_Views DESC 





