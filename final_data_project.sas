libname nba '/home/jsomers20/STAT 440/Last Final';

/*read in datasets*/
data nba.nba2012;
	infile '/home/jsomers20/STAT 440/Last Final/2012.csv' missover firstobs=2 dsd;
	input Rank Player : $40. Position $ Age Team : $10. G GS MP FG FGA FG_Pct ThreePoints 
	      ThreePoints_A ThreePoints_Pct TwoPoints TwoPoints_A TwoPoints_Pct 
	  	  eFG_Pct FT FTA FT_Pct ORB DRB TRB AST STL BLK TOV PF Pts_G;
run;
 
data nba.nba2013;
	infile '/home/jsomers20/STAT 440/Last Final/2013.csv' missover firstobs=2 dsd;
	input Rank Player : $40. Position $ Age Team: $10.G GS MP FG FGA FG_Pct ThreePoints 
	      ThreePoints_A ThreePoints_Pct TwoPoints TwoPoints_A TwoPoints_Pct 
	  	  eFG_Pct FT FTA FT_Pct ORB DRB TRB AST STL BLK TOV PF Pts_G;
run;

data nba.nba2014;
	infile '/home/jsomers20/STAT 440/Last Final/2014.csv' missover firstobs=2 dsd;
	input Rank Player : $40. Position $ Age Team : $10.G GS MP FG FGA FG_Pct ThreePoints 
	      ThreePoints_A ThreePoints_Pct TwoPoints TwoPoints_A TwoPoints_Pct 
	  	  eFG_Pct FT FTA FT_Pct ORB DRB TRB AST STL BLK TOV PF Pts_G;
run;

data nba.nba2015;
	infile '/home/jsomers20/STAT 440/Last Final/2015.csv' missover firstobs=2 dsd;
	input Rank Player : $40. Position $ Age Team : $10. G GS MP FG FGA FG_Pct ThreePoints 
	      ThreePoints_A ThreePoints_Pct TwoPoints TwoPoints_A TwoPoints_Pct 
	  	  eFG_Pct FT FTA FT_Pct ORB DRB TRB AST STL BLK TOV PF Pts_G;
run;
 
data nba.nba2016;
	infile '/home/jsomers20/STAT 440/Last Final/2016.csv' missover firstobs=2 dsd;
	input Rank Player : $40. Position $ Age Team : $10. G GS MP FG FGA FG_Pct ThreePoints 
	      ThreePoints_A ThreePoints_Pct TwoPoints TwoPoints_A TwoPoints_Pct 
	  	  eFG_Pct FT FTA FT_Pct ORB DRB TRB AST STL BLK TOV PF Pts_G;
run;
 
 
/*general cleaning and dataset concatination*/
proc sql;
	create table nba.nba(drop=Player drop=Rank) as 
      	select *,2012 as Year, scan(Player, 1, '\') as Player_Name, round(ThreePoints*G) as ThreePoints_Tot, round(Pts_G*G) as Pts_Tot  from nba.nba2012
      	union
      	select *,2013 as Year, scan(Player, 1, '\') as Player_Name, round(ThreePoints*G) as ThreePoints_Tot, round(Pts_G*G) as Pts_Tot  from nba.nba2013
      	union
      	select *,2014 as Year, scan(Player, 1, '\') as Player_Name, round(ThreePoints*G) as ThreePoints_Tot, round(Pts_G*G) as Pts_Tot  from nba.nba2014
      	union
      	select *,2015 as Year, scan(Player, 1, '\') as Player_Name, round(ThreePoints*G) as ThreePoints_Tot, round(Pts_G*G) as Pts_Tot  from nba.nba2015
      	union
      	select *,2016 as Year, scan(Player, 1, '\') as Player_Name, round(ThreePoints*G) as ThreePoints_Tot, round(Pts_G*G) as Pts_Tot  from nba.nba2016;
      	
	reset double;
	title 'NBA Data';
 
	alter table nba.nba
		modify  Year format=8., 
		        Player_Name label = 'Player'format=$40., 
				G label = 'Games Played', 
				GS label = 'Games Started', 
				MP label = 'Average Minutes Played per Game',
				FG label = 'Field Goals Made', 
				FGA label = 'Field Goal Attempts per Game', 
				FG_Pct label = 'Field Goal Percentage' format=percent8.3 ,
				ThreePoints label = '3-Pointers Made', 
				ThreePoints_A label = '3-Pointers Attempts', 
				ThreePoints_Pct label = '3-Pointers Percentage' format=percent8.3 ,
				TwoPoints label = '2-Pointers Made' , 
				TwoPoints_A label = '2-Pointers Attempted', 
				TwoPoints_Pct label = '2-Pointers Percentage' format=percent8.3,
				eFG_Pct label = 'Effective Field Goal Percentage' format=percent8.3, 
				FT label = 'Free Throws Made', 
				FTA label = 'Free Throw Attempts', 
				FT_Pct label = 'Free Throw Percentage' format=percent8.3,
				ORB label = 'Offensive Rebounds per Game',
				DRB label = 'Defensive Rebounds per Game', 
				TRB label = 'Total Rebounds per Game',
				AST label = 'Assists per Game', 
				STL label = 'Steals per Game', 
				BLK label = 'Blocks per Game', 
				TOV label = 'Turnovers per Game',
				PF label = 'Personal Fouls per Game', 
				Pts_G label = 'Points per Game', 
				ThreePoints_Tot label= 'Total 3-Pointers', 
				Pts_Tot label= 'Total Points';
quit;


/*intial error exploration*/
ods select Variables Attributes;
proc contents data = nba.nba;
run;
ods select default;

/*check for overall missing values*/
title2 "Overall Missing Levels";
proc freq data=nba.nba nlevels;
   tables _all_ / noprint;
run;

/*identify the unusual levels in teams*/
title2 "Unusual Levels in Teams";
proc freq data=nba.nba;
	tables Team;
run;

/*identify the unusual levels in position*/
title2 "Unusual Levels in Position";
proc freq data=nba.nba;
	tables Position;
run;

/*check for the range of total games per player*/
title2 "Unusual Total Games";
proc means data=nba.nba   fw=12;
   var G;
   class Player_Name;
   where G>82 or G <0;
run;  

/*cleaning missing values*/
data nba.nba_clean;
	set nba.nba;
	FG_Pct = FG / FGA; 
	ThreePoints_Pct = ThreePoints / ThreePoints_A; 
	TwoPoints_Pct = TwoPoints / TwoPoints_A ;
	FT_Pct = FT / FTA; 
	if FG_Pct=. then FG_Pct=0;
	if ThreePoints_Pct=. then ThreePoints_Pct=0;
	if TwoPoints_Pct=. then TwoPoints_Pct=0;
	if eFG_Pct=. then eFG_Pct=0;
	if FT_Pct=. then FT_Pct=0;
	if Team="CHO" or Team="CHA" then Team= "CHO/CHA";
	if Team="NOH" or Team="NOP" then Team="NOH/NOP";
run;

/*confirms fixes were made*/
title2 "Confirms Miscellaneous Fixes";
proc freq data=nba.nba_clean nlevels;
   tables _all_ / noprint;
run; 

/*confirms fixes for renamed franchises*/
title2 "Confirms Fixes for Renamed Franchises";
proc freq data=nba.nba_clean;
	tables Team;
run;



/*count unique teams */
title2 "Unique Teams per Player (Top 10 Obs.) ";
proc sql outobs=10;
	select Player_Name, count(distinct Team) as Distinct_Teams from nba.nba_clean where Team^= "TOT" 
	group by Player_Name order by Distinct_Teams desc;
quit;

/*points and three point totals data sets created*/
proc sort data=nba.nba_clean
	out=nba.nbasort;
	by Player_Name;
run;
data nba.ThreePoints(keep=Player_Name ThreePoints_Player) nba.TotalPoints(keep=Player_Name TotalPoints_Player);
	set nba.nbasort;
	label ThreePoints_Player = "Three Point Total" TotalPoints_Player = "Total Points";
	format ThreePoints_Player TotalPoints_Player COMMA9.;
	by Player_Name;
	if First.Player_Name then do; 
	   ThreePoints_Player=0;
	   TotalPoints_Player=0;
	end;
	ThreePoints_Player + ThreePoints_Tot;
	TotalPoints_Player + Pts_Tot;
	if Last.Player_Name then output nba.Threepoints nba.TotalPoints; 
run;


/*points leader by team*/
proc means data=nba.nba_clean(where= ( Team^="TOT" )) nway noprint;
   class Team Player_Name;
   var Pts_Tot;
   output out=Pts_Total
          sum=Total_Pts;
run;
proc means data=Pts_Total nway noprint;
   class Team;
   var Total_Pts;
   output out=Pts_max (drop=_TYPE_ _FREQ_)
   		  max=Max_Pts;
run;
data TotalPoints_Team;
   merge Pts_Total Pts_max;
   by Team;
   if Total_Pts=Max_Pts;
   
   keep Team Player_Name _FREQ_ Total_Pts;
run;
proc sort data=TotalPoints_Team;
   by descending Total_Pts;
   run;
title2"Points Leader Per Team";
proc print data = TotalPoints_Team(rename=(_FREQ_=Years_Freq)) label noobs;
	format Total_Pts COMMA9.;
	label Years_Freq = 'Number of Years' ThreePoints_Player = "Three_Point_Total";
run;

/*SQL used to print out points, 3-points, FG%, rebounds, and 20-5-5 metric leaders*/
proc sql outobs = 10 ;
	title 'Top 10 Three Points shooters';
	select * from nba.ThreePoints order by ThreePoints_Player desc;
	
	title 'Top 10 Points Leaders';
	select * from nba.TotalPoints order by TotalPoints_Player desc;
	
	title 'Top 10 Best Field Goal Shooting Percentages'; 
	select Player_Name, Position, Year, eFG_Pct, FGA format = 8.1 from nba.nba_clean where FGA > 3 order by eFG_Pct desc;
	
	title 'Top 10 Number of Defensive Rebounds';
	select Player_Name, Year, DRB format = 8.1 from nba.nba_clean order by DRB desc; 
quit;
proc sql;
	title 'Players with 20 Points Per Game, 5 Rebounds Per Game, 5 Assists Per Game';
	select Player_Name, Position, Year, TRB format = 8.1, Pts_G format = 8.1, AST format = 8.1 from nba.nba_clean where TRB > 5 and AST > 5 and Pts_G > 20;
quit;


