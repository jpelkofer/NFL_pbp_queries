register  /usr/hdp/current/pig-client/lib/piggybank.jar;
nfl = LOAD 'hdfs:/user/maria_dev/final/nfl/NFL Play by Play 2009-2017 (v4).csv' using org.apache.pig.piggybank.storage.CSVExcelStorage();
nfl2017 = FILTER nfl BY (int)$101 == 2017 and (int)$4 == 3 and (int)$12 <= 4;
--Quaters 1 and 3
qtrs1and3 = FILTER nfl2017 BY $3 == 1 or $3 == 3;
noPlay1and3 = FILTER qtrs1and3 BY $29 != 'No Play'; --Filter out no play
qtrOneAndThree = FOREACH noPlay1and3 GENERATE $16 as team, $29 as play;
--4th Qtr
qtr4 = FILTER nfl2017 BY $3 == 4 and $6 > 5;
noPlay4 = FILTER qtr4 BY $29 != 'No Play'; --Filter out no play
qtrfour = FOREACH noPlay4 GENERATE $16 as team, $29 as play;
--2nd Qtr
qtr2 = FILTER nfl2017 BY $3 == 2 and $6 > 3;
noPlay2 = FILTER qtr2 BY $29 != 'No Play'; --Filter out no play
qtrtwo = FOREACH noPlay2 GENERATE $16 as team, $29 as play;

allPlays = UNION qtrOneAndThree, qtrfour, qtrtwo;

grpTeam = GROUP allPlays BY team;
counts = FOREACH grpTeam {
				 run = FILTER allPlays BY play == 'Run';
				 GENERATE group as team, COUNT($1.team) as allPlays, SIZE(run) as runPlays;
}

percentages = FOREACH counts GENERATE team, (float)runPlays/(float)allPlays as runPerc, ((float)allPlays-(float)runPlays)/(float)allPlays as passPerc;
sortByRun = ORDER percentages BY runPerc desc;
DUMP sortByRun;