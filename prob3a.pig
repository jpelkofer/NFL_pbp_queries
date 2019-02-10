register  /usr/hdp/current/pig-client/lib/piggybank.jar;
nfl = LOAD 'hdfs:/user/maria_dev/final/nfl/NFL Play by Play 2009-2017 (v4).csv' using org.apache.pig.piggybank.storage.CSVExcelStorage();
passPlays = FILTER nfl BY $29 == 'Pass' or $29 == 'Sack';
tmAndSeason = FOREACH passPlays GENERATE CONCAT((chararray)$17, ':', (chararray)$101) as team, $29 as play, $37 as qbhit;
grpTeams = GROUP tmAndSeason BY team;
percHits = FOREACH grpTeams GENERATE group as team, COUNT($1.play) as plays, SUM($1.qbhit) as qbhits, SUM($1.qbhit)/COUNT($1.play) as qbHitperc;
spltSeason = FOREACH percHits GENERATE FLATTEN(STRSPLIT(team, ':')) as (team:chararray, season:chararray), qbHitperc;
grpSeasons = GROUP spltSeason BY season;
topBYSeason = FOREACH grpSeasons {
								  sorted = ORDER spltSeason BY qbHitperc desc;
                                  top1 = limit sorted 1;
                                  generate flatten(top1);
}
DUMP topBYSeason;