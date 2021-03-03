%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas';
*update above line with appropritate directory information;


title " ";
proc print data=data.&linkfn.0_mtch(obs=10);
proc print data=data.&linkfn.1(obs=10);
proc print data=data.&linkfn.2(obs=10);        

proc sort data=data.&linkfn.0_mtch;
by &id1 &id2;

proc sort data=data.&linkfn.1;
by &id1 &id2;

proc sort data=data.&linkfn.2;
by &id1 &id2;

proc sort data=data.&linkfn._hit;
by &id1 &id2;

data data.&linkfn(rename=(dist=tdist) );
merge data.&linkfn.0_mtch(in=aa) data.&linkfn.1(in=bb)
    data.&linkfn.2(in=cc) data.&linkfn._hit(in=dd);
by &id1 &id2;
keep &id1 &id2 mtype dist prob;

if aa then mtype=0;
else if bb then mtype=2;
else if cc then mtype=3;
else if mtch>0 then mtype=4;
else mtype=5;

if aa then dist=dist; *to keep aa same;
else if dd then dist=1000+mtch;
else if bb then dist=2000;
else if cc then dist=3000;

proc sort nodupkey;
    by &id1 &id2;

proc freq;
tables mtype;    

proc sort data=&db1.0;
    by &id1;
    
data data.&linkfn._full;
    merge data.&linkfn(in=aa) &db1.0;
    by &id1;
    if ~aa then mtype=-21;

*proc print data=data.&linkfn._full(obs=20);
*    where &id2~='';
    
proc sort;
by &id2 &id1;

proc sort data=data._db20;
    by &id2;

*xproc print data=data._db20(obs=20);

data data.&linkfn._full;
    merge data.&linkfn._full(in=aa) &db2.0(rename=(zip=zip2 name=name2 addr=addr2));
    by &id2;
    if ~aa then mtype=-22;

if zip~=zip2 then dzip=1;
else dzip=0;

proc freq;
tables mtype*dzip/nocol norow nopercent;
where _droprow=.;

proc print data=data.&linkfn._full(obs=20);
where dzip=0 and mtype=3;
    
%writecsv(infn=data.&linkfn._full, outfn=\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\&linkfn.&iter.mtch);

%_count(data.&linkfn._full, &id1, n&id1);    
%_count(data.&linkfn._full, &id2, n&id2);

proc freq data=data.&linkfn._full;
    tables tdist n&id1 n&id2;
where _droprow=.;

proc freq data=data.&linkfn._full;
    tables tdist n&id1 n&id2;

proc print data=data.&linkfn._full(obs=10);;    
where n&id2>1;
run;

    

