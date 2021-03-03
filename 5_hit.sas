%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas';
*update above line with appropritate directory information;


options mprint;


*before reading-in this file, manual checking was done to generate the sheet 'mlink';
%readxlsx(infn=&linkfn.2hit_m.xlsx, sht=mlink, outfn=data.mlink); 

data data.&linkfn._hit;
    set data.mlink;
    keep fid mcareid mtch;
    where fid~='' or mtch<0;
*if 0<mtch<100 then mtch=1; * for sure;
if mtch<-10 then mtch=-10; *no accute bed;
*else if mtch<0 then mtch=-1; * mult;
else mtch=100; *probably;

/*match code used:
1=sure
-1=mult
100=probably
*/

    
proc sort nodupkey;
by &id1 &id2;

proc freq;
    tables mtch;

proc print data=data.&linkfn._hit(obs=30);
run; 
    
