
%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas'; 
*update above line with appropritate directory information;


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*;

 proc sort data=data.mcareid0;
 by mcareid _droprow;
 *proc print data=&&db&n..&iter(obs=10);

run;

      
 data data._db10 _srcdb10;
	 retain _umcareid;
	 set data.mcareid0;
	 by mcareid;
	 * if _droprow<1 then _u&&id&n =_N_;
	 if first.mcareid then _umcareid =_N_; 
	 *_N_: data step iteration number/obs no. --mk;

run;

   

 proc sort data=data._db10;
 by zip;

run;
    

 proc sort data=data.fid0;
 by fid _droprow;

run;
      

 data data._db20 _srcdb20;
 retain _ufid;
 set data.fid0;
 by fid;
 
 if first.fid then _ufid =_N_;
 rename name=name2 addr=addr2 ;

run;

     

 proc sort data=data._db20;
 by zip;

run;

*------------------------------------------------------------*;
*end block, preped mcareid0 & fid0 to merge;
*via mcareid0->_db10 & fid0->_db20;
*_umcareid and _ufid acts as a unique id for an entity...;
*...each iteration of the same entity gets the same _umcareid/_fid;
*_umcareid/_fid are derived from the obs # in the data file...;
*...not derived from actual id;
*------------------------------------------------------------*;

* block starts;
*------------------------------------------------------------*; 

  proc sql;
	 create table data.mcareid2fid0 as 
	select _umcareid, _ufid, mcareid, fid , _db10.zip as zip , name, name2 , addr, addr2 
	from data._db10 as _db10, data._db20 as _db20 
	where ( (_db10.zip=_db20.zip) AND 1); *mcareid2fid0 iter1 --mk; * , _db20.in_dshs as in_dshs;;

  quit;

run;

*------------------------------------------------------------*;
*end block, merged _db10 & _db20;
*selected vars are now in mcareid2fid0 (iter1);
*OBS W/ ALL COMMON ZIPs LINKED TO EACH OTHER;
*------------------------------------------------------------*;

* block starts;
*------------------------------------------------------------*; 


/*
  proc print data=data.mcareid2fid0(obs=5);

   

  proc print data=data._db10(obs=5);
  * where find (name, 'woodlands')>0; *why ‘woodlands’? just random check?;
    

  proc print data=data._db20(obs=5);
  * where find (name, 'woodlands')>0;
 ;
  * compare;
*/
      

  data data.mcareid2fid0;
  set data.mcareid2fid0;
	  exactsm=0;
	  approxsm=0;
	  if compress(name)~='' then do;
	  	if name=name2 then exactsm=exactsm+10**0; 
		*name exact matches gets score 1 (ie. exactsm=1);
	  end;
	  if compress(addr)~='' then do;
	  	if addr=addr2 then exactsm=exactsm+10**1; 
		*address exact match gets score 11 if name is also exact match,..;
		*..10 if name is not exact match;
	  end;

	  if exactsm=0 then do;
		 if compress(name)~='' then do;
			 ***** ADD code for approximate match for var1;
			  _i=1;
			  _ilen=0;
			  _jlen=0;
			  _nwrd=0;
			  do while (scan(name, _i)~='');
			  _ilen=_ilen+1;
			  _j=1;
			  do while (scan(name2, _j)~='');
			  if _ilen=1 then _jlen=_jlen+1;
			  if scan(name, _i)=scan(name2, _j) then _nwrd=_nwrd+1;
			  _j=_j+1;
	  	end;

	  _i=_i+1;
	  end;

	  drop _i _j _ilen _jlen;
	 ;
	  _nwrd=(2*_nwrd)/(_ilen+_jlen);
	 ;
	 if length(name)<length(name2) then _sub=find(name2, name);
	 else _sub=find(name, name2);
	 if _nwrd>(2/3-0.001) or _sub>1 then approxsm=approxsm+10**0;
	 drop _nwrd _sub;
	 ;
	 end;
	  if compress(addr)~='' then do; *why not used for addr2? --mk;
	 ***** ADD code for approximate match for var2;
	 _v1=tranwrd(addr, ' rd', '');
	 _v1=tranwrd(_v1, ' st', '');
	 _v1=tranwrd(_v1, ' ave', '');
	 _v1=tranwrd(_v1, ' dr', '');
	 _v1=tranwrd(_v1, ' blvd', '');
	 _v1=tranwrd(_v1, ' ln', '');
	 _v1=tranwrd(_v1, ' cir', '');
	 _v1=tranwrd(_v1, ' lp', '');
	 ;
	 _v2=tranwrd(addr2, ' rd', '');
	 _v2=tranwrd(_v2, ' st', '');
	 _v2=tranwrd(_v2, ' ave', '');
	 _v2=tranwrd(_v2, ' dr', '');
	 _v2=tranwrd(_v2, ' blvd', '');
	 _v2=tranwrd(_v2, ' ln', '');
	 _v2=tranwrd(_v2, ' cir', '');
	 _v2=tranwrd(_v2, ' lp', '');
	 ;
	  _i=1;
	  _ilen=0;
	  _jlen=0;
	  _nwrd=0;
	  
	  do while (scan(_v1, _i)~='');
		  _ilen=_ilen+1;
		  _j=1;
			  do while (scan(_v2, _j)~='');
			  if _ilen=1 then _jlen=_jlen+1;
			  if scan(_v1, _i)=scan(_v2, _j) then _nwrd=_nwrd+1;
			  _j=_j+1;
			  end;
		  _i=_i+1;
	  end;
	  drop _i _j _ilen _jlen;
	 ;
	  _nwrd=(2*_nwrd)/(_ilen+_jlen);
	 ;
	 if _nwrd>(2/3-0.001) then approxsm=approxsm+10**1;
	 drop _nwrd _v1 _v2;
	 ;
	  end;
	  end;
	  
	  * exact: 1 same for var1, 10 same for var2 etc;
	  * 11 match on both;
	  * min is good;
	  dist=100;
	  if mod(exactsm,10)=1 and exactsm>1 then dist=1;
	  * var1 & others same;
	  else if exactsm=1 then dist=2;
	  * var1 same;
	  else if exactsm>1 then dist=3;
	  * other vars the same;
	  else if mod(approxsm,10)=1 and approxsm>1 then dist=10;
	  * var1 & others approx;
	  else if approxsm=1 then dist=20;
	  * var1 approx;
	  else if approxsm>1 then dist=30;
	  * other vars the approx;

run;

*------------------------------------------------------------*;
*end block, mcareid2fid0 (iter1) -> mcareid2fid0 (iter2);
*....;
*EACH OBS IS SCORED BASED ON LINK TYPE;
*------------------------------------------------------------*;


  proc freq data=data.mcareid2fid0;
  tables exactsm approxsm;

run;

      
/*
  proc print data=data.mcareid2fid0(obs=50);
  title "DEBUG";
  where (mcareid in ('452018','450135') or fid in ('4396163','4391440') ) and exactsm+approxsm>0;
  *where &byvar in ("&PRNTBYVAL");
  *where (mcareid in ('452018') or fid in ('4396163') ) *proc print data=data.&linkfn&iter(obs=5);
  * where approxsm>0;

*/


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 

  proc sort;
  by zip _umcareid _ufid;

run;
      

  proc summary;
  by zip _umcareid _ufid;
  var dist;
  output out=data.mcareid2fid0(drop=_type_ _freq_) min=;

run;    

*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*;

* block starts;
*------------------------------------------------------------*; 

  data data.mcareid2fid0;
  set data.mcareid2fid0;
  linkid=_N_;

run;
  
*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*; 

/*
  proc print data=data.mcareid2fid0(obs=50);
  title "DEBUG";
  *where &byvar in ("&PRNTBYVAL") and dist<100;
  where (mcareid in ('452018','450135') or fid in ('4396163','4391440') );
ERROR: Variable mcareid is not on file DATA.MCAREID2FID0.
  *proc print data=data.&linkfn&iter(obs=5);
  * where approxsm>0;

NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE PRINT used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
*/


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 


 proc sort data=data.mcareid2fid0(keep=linkid _umcareid dist) out=_tmp;
 by _umcareid;
run;
      

 proc sort data=data._db10;
 by _umcareid;
run;
      

 proc sql;
	 create table _tmp2 as 
	select * 
	from data._db10 as t1, _tmp as t2 
	where (t1._umcareid=t2._umcareid);

run;

/*
data dta_chk._tmp2__iter1;
	set _tmp2;
run;;;
*/
 
*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*;


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 

 proc sort;
 by zip _umcareid _droprow dist;
run;
      

 proc summary;
	 by zip _umcareid _droprow;
	 var dist;
	 output out=_tmp3(drop=_freq_ _type_) min=;
	 * Grab the closest. If multple, grab all;
run;

/*     
data dta_chk._tmp3__iter1;
	set _tmp3;
run;;;
*/

 proc sort data=_tmp3;
 by zip _umcareid _droprow dist;
run;


 data _tmp2;
 merge _tmp2 _tmp3(in=aa);
	 by zip _umcareid _droprow dist;
	 if aa;

run;

/*
data dta_chk._tmp2__iter2;
	set _tmp2;
run;;;
*/

*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*; 


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 

 proc sort data=_tmp2;
 by _umcareid _droprow;

run;
      

 proc summary;
	 by _umcareid _droprow;
	 output out=_tmp3(drop=_type_); 
	 *w/o var statement proc summary only counts --mk;
run;
    
/*
data dta_chk._tmp3__iter2;
	set _tmp3;
run;;; 
*/

*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*; 


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 

 proc sort data=data._db10;
	 by _umcareid _droprow;
	 * proc print data=data._db&n&iter (obs=3);
	 * where &&id&n in ('450730','450669');
	 * proc print data=_tmp2(obs=3);
	 * where &&id&n in ('450730','450669');
	 * nlinkid: how many tied minimum links. if >100 then minimum is no link;
run;
      

 data data._db10;
 merge _tmp2(keep=_umcareid _droprow dist linkid) _tmp3(rename=(_freq_=nlinkid)) data._db10;
	 by _umcareid _droprow;
	 if dist=. then dist=1000;
	 if dist=100 then nlinkid=nlinkid+100;

run;


*------------------------------------------------------------*;
* block ends;
*------------------------------------------------------------*; 
      

 proc sort;
	by zip dist linkid mcareid _droprow;
run;
      

 proc freq;
	 tables dist nlinkid;
	 where _droprow=.;

run;

 proc print data=data._db10(obs=10);
	where nlink1>1 and dist<100;
run;

 ;

      
*/


*------------------------------------------------------------*;
* block starts;
*------------------------------------------------------------*; 

 proc sort data=data.mcareid2fid0(keep=linkid _ufid dist) out=_tmp;
 by _ufid;
run;
      

 proc sort data=data._db20;
 by _ufid;
run;
      

 proc sql;
 create table _tmp2 as select * from data._db20 as t1, _tmp as t2 where (t1._ufid=t2._ufid);

run;
      

 proc sort;
 by zip _ufid _droprow dist;
run;
      

 proc summary;
	 by zip _ufid _droprow;
	 var dist;
	 output out=_tmp3(drop=_freq_ _type_) min=;
	 * Grab the closest. If multple, grab all;
run;
      

 data _tmp2;
 merge _tmp2 _tmp3(in=aa);
	 by zip _ufid _droprow dist;
	 if aa;
	 rename name2=name addr2=addr ;

run;
      

 proc sort;
	by _ufid _droprow;

run;
      

 proc summary;
	 by _ufid _droprow;
	 output out=_tmp3(drop=_type_);
run;
      

 proc sort data=data._db20;
	 by _ufid _droprow;
	 * nlinkid: how many tied minimum links. if >100 then minimum is no link;

run;
      

 data data._db20;
 merge _tmp2(keep=_ufid _droprow dist linkid) _tmp3(rename=(_freq_=nlinkid)) data._db20;
	 by _ufid _droprow;
	 if dist=. then dist=1000;
	 if dist=100 then nlinkid=nlinkid+100;

run;
      
    
*------------------------------------------------------------*;

 proc sort;
	by zip dist linkid fid _droprow;
run;
      

 proc freq;
	 tables dist nlinkid;
	 where _droprow=.;

run;
      

 proc print data=data._db20(obs=10);
	where nlink2>1 and dist<100;

run;
      

*------------------------------------------------------------*;


data _linkfn;
  set data._db10 data._db20 (rename=( name2=name addr2=addr ));
  by zip dist linkid;
  
run;
   
  
 proc sort data=_linkfn; *sorting WORK._LINKFN;
 by zip linkid;

run; 


/*
data dta_chk._linkfn__iter1;
	set _linkfn;
run;;; 
*/

*------------------------------------------------------------*;
      

 proc summary data=_linkfn;
	 by zip linkid;
	 where _droprow<1 and dist<100 and nlinkid=1;
	 output out=data.clust(drop=_type_ rename=(_freq_=nmtch));

run;
      
*------------------------------------------------------------*;


 data _linkfn;
 merge _linkfn data.clust;
	 by zip linkid;
	 if nmtch=2 then mtch=1;
	 else mtch=0;

run;

/*
data dta_chk._linkfn__iter2;
	set _linkfn;
run;;; 

*/
*------------------------------------------------------------*;
      

 proc summary data=_linkfn; 
	 by zip;
	 where _droprow<1;
	 var _umcareid _ufid mtch;
	 output out=data.clust(drop=_type_ rename=(_freq_=nclust)) N(_umcareid)=nmcareid N(_ufid)=nfid sum(mtch)=nlinks ;

run;
 

*------------------------------------------------------------*; 

 data data.clust;
 set data.clust;
	 nlinks=nlinks/2;
	 chk=0;
	 prob='3L';
	 if nmcareid=0 then prob='N1';
	 * only in db2;
	 else if nfid=0 then prob='N2';
	 * only in db1;
	 else if nmcareid=nlinks then do;
	 prob='2P';
	 if nlinks=1 and nclust=nlinks*2 then prob='1H';
	 chk=1;
	 end;
	 *if int(nlinks/100)/2=1 and n&id1=1 then prob='2P';
	 * 1 to 1 approx;

run;
      
*------------------------------------------------------------*; 
*------------------------------------------------------------*; 


 proc freq data=data.clust;
 tables prob nclust;

run;
      
*------------------------------------------------------------*;

 data _linkfn;
	 merge _linkfn data.clust;
	 *(keep=&byvar ctype);
	 by zip;
	 ;

run;

/*
 data dta_chk._linkfn__iter3;
 merge _linkfn;
 *(keep=&byvar ctype);
 by zip;
 ;

run;
*/
      

  proc sql;
	  create table data.mcareid2fid0 as 
	 select chk, mtch, linkid, nclust, prob, nmcareid, nfid, nlinks, dist, 
	 _droprow , zip , fid , mcareid , fname , name , addr , _umcareid, _ufid
	 from _linkfn; *in_dshs , ;;
run;   

*------------------------------------------------------------*; 

  proc sort data=data.mcareid2fid0;
	by zip linkid descending mcareid fid;
run;
      

  data data.mcareid1(drop=mtch);
	  set data.mcareid2fid0(drop=chk linkid nclust prob nmcareid nfid nlinks _umcareid _ufid fid);
	  where mtch=0 and mcareid~='';

run;
*--------------------------*;

  proc sort data=data.mcareid1 nodupkey;
	  by _all_;
	  *proc print data=&db1%eval(&iter+1)(obs=10);
run;

*------------------------------------------------------------*; 

  data data.fid1;
	  set data.mcareid2fid0(drop=chk linkid nclust prob nmcareid nfid nlinks _umcareid _ufid mcareid);
	  where mtch=0 and fid~='';

run;
*--------------------------*;      

  proc sort data=data.fid1 nodupkey;
	  by _all_;
	  *proc print data=&db2%eval(&iter+1)(obs=10);

run;


							data data.fid1;
							set data.fid1;
								in_dshs=.;
							run;
 
*------------------------------------------------------------*; 

  proc sort data=data.mcareid2fid0(keep=linkid mcareid fid prob mtch dist _droprow) out=data.mcareid2fid0_mtch;
	  by linkid;
	  where mtch=1 and _droprow=.;
run;
   
*--------------------------*; 

  data data.mcareid2fid0_mtch;
  merge data.mcareid2fid0_mtch(keep=linkid mcareid prob dist where=(mcareid ~= '')) 
		data.mcareid2fid0_mtch(keep=linkid fid prob dist where=(fid ~= ''));
  by linkid;

run;
      
*------------------------------------------------------------*;


/*
  proc print data=data.mcareid2fid0_mtch(obs=50);
	  title "DEBUG";
	  where (mcareid in ('452018','450135') or fid in ('4396163','4391440') );
	  title;
  
run;
*/

proc freq data=data.&linkfn&iter._mtch;
             tables dist;
run;

proc freq data=data.&linkfn&iter;
         where dist<100 and _droprow=. and mtch=1;
         tables dist;
		 run;
         
data approx;
         set data.&linkfn&iter;
         where 100>dist>=10 and mtch=1;
         %writecsv(infn=approx, outfn=H:\record_link_RA_work\OrgLink files copied\7_mkarim_2018_replicate\raw\&linkfn&iter.approx);
*replace folder name w/ appropriate directory information;
run;

 data data.&linkfn&iter data.&linkfn&iter.x data.&linkfn&iter.chk
            data.&linkfn&iter._&id1 data.&linkfn&iter._&id2;
            ;
        set data.&linkfn&iter;
            if prob in ('N1') then output data.&linkfn&iter.x;
            else if prob in ('1H', '2P') then output data.&linkfn&iter;
            else output data.&linkfn&iter.chk;
        
            if mtch=1 and _droprow=. then do;
                if &id1~='' then output data.&linkfn&iter._&id1;
                else output data.&linkfn&iter._&id2;
            end;
run;

proc freq data=data.&linkfn&iter.chk;
            where &id1~='';
            tables mtch;
run;


proc sort data=data.&linkfn&iter;
       by descending prob;
       %writecsv(infn=data.&linkfn&iter, outfn=H:\record_link_RA_work\OrgLink files copied\7_mkarim_2018_replicate\raw\&linkfn&iter.lnk);
	   %writecsv(infn=data.&linkfn&iter.chk, outfn=H:\record_link_RA_work\OrgLink files copied\7_mkarim_2018_replicate\raw\&linkfn&iter.chk);
	   %writecsv(infn=data.&linkfn&iter.x, outfn=H:\record_link_RA_work\OrgLink files copied\7_mkarim_2018_replicate\raw\&linkfn&iter.x);
*replace folder name w/ appropriate directory information for each csv file above;
run;

proc freq data=data.&clustfn;
            title "ignore clusters that do not invovle waiver";
            tables nclust prob;
            where prob~='N1';
run;


title ""; run;
