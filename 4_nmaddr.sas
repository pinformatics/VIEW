%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas';
*update above line with appropritate directory information;



options mprint;

* all var type as strings;
ods html body="4_nmaddr.html";
title " ";

%let iter=1;
%let byvar=name;  * block vars;
%let vnum=0;
%_exactmtch;

%let iter=2;
%let byvar=addr2;  * block vars;

data &db1&iter;
    set &db1&iter;
%_dropaddr(invar=addr, outvar=addr2);
addr2=compress(addr2);

data &db2&iter;
    set &db2&iter;
%_dropaddr(invar=addr, outvar=addr2);
addr2=compress(addr2);

%_exactmtch;

data approx;
    set  data.&linkfn.1  data.&linkfn.2;
    rename addr2=addr;
    
proc sort;
by &id1;

data approx;
    merge approx(in=aa) &db1.0(keep=&id1 addr name rename=(addr=addr1 name=name1));
by &id1;    
    if aa;

proc sort;
by &id2;

data approx;
    merge approx(in=aa) &db2.0(keep=&id2 addr name rename=(addr=addr2 name=name2));
by &id2;    
    if aa;
if ~(addr='' and compress(addr1)=compress(addr2) );
    
%writexlsx(infn=approx, outfn=\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\&linkfn.&iter.approx); 

data data.hit;
    set &db1.3 &db2.3;

%writexlsx(infn=data.hit, outfn=\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\&linkfn.&iter.hit);
run;
ods html close;
    
