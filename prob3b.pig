register  /usr/hdp/current/pig-client/lib/piggybank.jar;
nfl = LOAD 'hdfs:/user/maria_dev/final/nfl/NFL Play by Play 2009-2017 (v4).csv' using org.apache.pig.piggybank.storage.CSVExcelStorage();

--Clutch Drives
clutchSituations = FILTER nfl BY $3 == 4 and $6 < 5 and $7 >= 50 and $69 >= -8 and $69 <= 0;
scoreNeeded = FOREACH clutchSituations GENERATE CONCAT($1, ':', $16) as gmTeam, $1 as gameID, $22 as TD, $54 as FG;
grpGames = GROUP scoreNeeded BY gmTeam;
scoreSuccess = FOREACH grpGames {
				   good = FILTER scoreNeeded BY FG == 'Good';
				   GENERATE group as gmTeam, 1 as cnt, SUM($1.TD) as TDcnt, SIZE(good) as FGcnt;
}

--Get QB's
QBs = FOREACH clutchSituations GENERATE CONCAT($1, ':', $16) as gmTeam, $30 as QB, $31 as QBid;
grpQBs = GROUP QBs BY gmTeam;
distinctQBs = FOREACH grpQBs {
					  qbName = DISTINCT $1.QB;
					  GENERATE group as gmTeam, qbName as QB;
}
gamesQBs = FOREACH distinctQBs GENERATE gmTeam, FLATTEN($1.QB) as QB;
filterQBs = FILTER gamesQBs BY QB != 'NA';

joinData = JOIN filterQBs BY gmTeam, scoreSuccess BY gmTeam;
QBgroup = GROUP joinData BY QB;
clutchCounts = FOREACH QBgroup GENERATE group as QB, SUM($1.cnt) as gms, SUM($1.TDcnt) as TDcnt, SUM($1.FGcnt) as FGcnt;
filterCnts = FILTER clutchCounts BY gms > 8;
clutchPerc = FOREACH filterCnts GENERATE QB, gms, (TDcnt + FGcnt)/gms as successRate, TDcnt, FGcnt;
sorted = ORDER clutchPerc BY successRate desc;

DUMP sorted;