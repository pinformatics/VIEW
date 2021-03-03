%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas';

libname in '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\data_mk';

%let fn=&linkfn;*.0_mtch;
%let byvar=&id1 &id2;

proc print data=data.&fn(obs=10);
proc print data=in.&fn(obs=10);

proc sort data=data.&fn;
    by &byvar;
    
proc sort data=in.&fn;
    by &byvar;

data t;
merge data.&fn(in=aa) in.&fn(in=bb);
    by &byvar;

    if ~(aa and bb);

    src=bb;

proc freq;
tables src;

proc print data=t(obs=100);
run;
