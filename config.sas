%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\lirbaries_at_mylink_mk\link_lib.sas';
%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\lirbaries_at_mylink_mk\orglink.sas';
%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\libraries_at_orglink2017_mk\config_stan_lib.sas';


/*libname dsrip '/opt/HPM/proj1/dsrip/data'; *NEEDS CORRECTION;
libname npi '/opt/HPM/proj1/npi/data'; *NEEDS CORRECTION;
libname mcare '/opt/HPM/proj1/hsr_mcare/data'; *NEEDS CORRECTION;*/

libname data '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\data_mk';
libname raw  '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw';

* OUTPUT;
* libname data location;
%let linkfn=mcareid2fid;
%let clustfn=clust;
    
* INPUT;
%let db1=data.mcareid;
%let id1=mcareid;
%let db2=data.fid;
%let id2=fid;
%let byvar=name;  * block vars;
%let vnum=3; * in order of importance;
%let var1=zip;
%let var2=addr;
%let var3=fname;
%let keepvar1=;
%let keepvar2=in_dshs;
%let othvar=; *npi1 in_dshs;
%let order=zip fid mcareid fname name addr in_dshs;
%let DEBUG=1;
* must only use either byvar of idvar;
%let DEBUG_COND=where (mcareid in ('452018','450135') or fid in ('4396163','4391440') );

*%let PRNTBYVAL=76104;
    
* nothing in first iteration;
* 1 in next itertation which incorporates confirm information;
%let iter=0;

*%include 'config_stan_lib.sas';;

%macro approx(v1, v2, idx, outvar);
    %let THRESHOLD=(2/3-0.001);
    %if &idx=1 %then %do;  ***** ADD code for approximate match for var1;
        %_sorensenwrd(&v1, &v2, _nwrd); *defined in link_lib.sas;

        if length(&v1)<length(&v2) then _sub=find(&v2, &v1);
        else _sub=find(&v1, &v2);
            
        if _nwrd>&THRESHOLD or _sub>1
            then &outvar=&outvar+10**%eval(&idx-1);

        drop _nwrd _sub;
    %end; %else %if &idx=2 %then %do; ***** ADD code for approximate match for var2;
        %_dropaddr(&v1, _v1);  *defined in link_lib.sas;
        %_dropaddr(&v2, _v2);

        %_sorensenwrd(_v1, _v2, _nwrd);
        if _nwrd>&THRESHOLD then &outvar=&outvar+10**%eval(&idx-1);
        drop _nwrd _v1 _v2;        
    %end; %else %if &idx=3 %then %do; ***** ADD code for approximate match for var3;        
*        if &v1=&v2 then &outvar=&outvar+10**%eval(&idx-1);        
    %end;    
%mend;

%macro readcsvf(infn, outfn);
    data &outfn;
        length chk 4. mtch linkid 8. nclust 4. prob $2. ntpi nfid nlinks dist _droprow 4. %orderfrmt _u&id1 _u&id2 8.;
        infile "&infn..csv" missover dsd firstobs=2;
        input chk mtch linkid nclust prob ntpi nfid nlinks dist _droprow &order _u&id1 _u&id2;
%mend;

*---------------------------------
read-write macros: Dr. Kum lib file
----------------------------------;

%macro readxlsx(infn, sht, outfn);
PROC IMPORT OUT= &outfn
            DATAFILE= "\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\&infn"
            DBMS=XLSX REPLACE;
     SHEET="&sht";    
     GETNAMES=YES;
RUN;

proc print data=&outfn(obs=10) headings=h;
proc contents data=&outfn;    
%mend;

%macro readxlsx2(infn, sht, outfn);
PROC IMPORT OUT= &outfn
            DATAFILE= "&infn"
            DBMS=XLSX REPLACE;
     SHEET="&sht";    
     GETNAMES=YES;
RUN;

proc print data=&outfn(obs=10) headings=h;
proc contents data=&outfn;    
%mend;

%macro writexlsx(infn, outfn);
PROC EXPORT DATA= &infn
            OUTFILE= "&outfn"
            DBMS=XLSX REPLACE;
RUN;
%mend;

%macro writecsv(infn, outfn);
PROC EXPORT DATA= &infn
            OUTFILE= "&outfn..csv"
            DBMS=CSV REPLACE;
RUN;
%mend;

%macro readcsv(infn, outfn);
    %readtxt(&infn, &outfn, CSV);
%mend;



