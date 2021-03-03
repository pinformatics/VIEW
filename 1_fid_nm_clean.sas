
%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas'; 
*update above line with appropritate directory information;



 proc import 
	datafile='\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\HospitalList12-2017.xlsx' 
	out=wfid_iter1 dbms=xlsx replace;
	getnames=yes; datarow=2;

run;

proc print data=wfid_iter1 (obs=10); title " ";
run;

proc contents data=wfid_iter1 (obs=10); title " ";
run;
      

data wfid_iter2	(where=(compress(name)~=''));
    set wfid_iter1(keep=fid city address facility zip   
	rename=( address=town fid=nfid)); *not in this infile: phone yr participate;
	    ******** standardize vars;
	    *%nzip5(invar=nzip, outvar=zip);


	 	length zip $10;
		format zip $5.;
		c_zip=compress(zip);
		zip=substr(c_zip, 1, 5);
	    if ~(0<zip<=99999) then zip='';
	   ;


	format fid $7.;
	    city=lowcase(compress(city));
	    fid=compress(nfid);
	    /* *no phone data in the infile;
		phone=nphone;
	    format phone $10.;
	    phone=substr(compress(phone, ' ().-'), 1, 10);
	    if ~(0<phone<=9999999999) then phone='';
	   ; 
		*/
	format _tmpvar $256.;
	format _src _des $256.;

	_src=compbl(town);
	    _des=_src;
	    _len=0;
	    do _i=1 to length(_src);
	    if ('0'<=substr(_src,_i, 1)<='9' or 'A'<=substr(_src,_i, 1)<='Z' or 'a'<=substr(_src,_i, 1)<='z' or substr(_src,_i, 1)=' ') then do;
	    if _len=0 then _des=substr(_src, _i, 1);
	    else _des=substr(_des, 1, _len)||substr(_src, _i, 1);
	    _len=_len+1;
	    end;
	    end;
	    _tmpvar=left(_des);
	    drop _src _des _len _i;
	   ;

	addr=lowcase(_tmpvar);
	    ***** ADD common words here. short words use spaces;
	    addr=tranwrd(addr, "lane", "ln");
	    addr=tranwrd(addr, "circle", "cir");
	    addr=tranwrd(addr, "avenue", "ave");
	    addr=tranwrd(addr, "drive", "dr");
	    addr=tranwrd(addr, "north", "n");
	    addr=tranwrd(addr, "south", "s");
	    addr=tranwrd(addr, "west", "w");
	    addr=tranwrd(addr, "east", "e");
	    addr=tranwrd(addr,"boulevard", "blvd");
	    addr=tranwrd(addr,"boulvard", "blvd");
	    addr=tranwrd(addr,"road","rd");
	    addr=tranwrd(addr,"street","st");
	    addr=tranwrd(addr,"avenue","ave");
	    addr=tranwrd(addr,"loop","lp");
	    addr=tranwrd(addr, "highway", "hwy");
	    addr=tranwrd(addr, "freeway", "fwy");
	    addr=tranwrd(addr, "parkway", "pkwy");
	    addr=tranwrd(addr,"3rd","third");
	    addr=tranwrd(addr,"2nd","second");
	    addr=tranwrd(addr, "ctr", "center");
	    addr=tranwrd(addr, "cntr", "center");
	    drop _tmpvar;
	   ;
	    ************ DATA CLEANING CODE;

		/*
	   *the infile contains only one column for facility name, so no need to use array ;
		array v{*} $ fac1 fac2 fac3;
	    do i= 1 to dim(v);
		*/
			    fname= facility; *v{i};
			    format _tmpvar $256.;
			    format _src _des $256.;
			    _src=compbl(fname);
			    _des=_src;
			    _len=0;

			    do _i=1 to length(_src);

					if ('0'<=substr(_src,_i, 1)<='9' or 'A'<=substr(_src,_i, 1)<='Z' or 'a'<=substr(_src,_i, 1)<='z' or 
					substr(_src,_i, 1)=' ') 
					then do;
				    if _len=0 then _des=substr(_src, _i, 1);
				    else _des=substr(_des, 1, _len)||substr(_src, _i, 1);
				    _len=_len+1;
			    	end;

				end;


_tmpvar=left(_des);
			    drop _src _des _len _i;
			   ;
			    name=lowcase(_tmpvar);
			    drop _tmpvar;
			   ;
			    name=tranwrd(name, " inc", " ");
			    name=tranwrd(name, " llp", " ");
			    name=tranwrd(name, " llc", " ");
			    name=tranwrd(name, " at ", " ");
			    name=tranwrd(name, " of ", " ");
			    name=tranwrd(name, "the ", " ");
			    * &outvar=tranwrd(&outvar, "memorial", " ");
			    * &outvar=tranwrd(&outvar, "regional", "");
			    name=tranwrd(name, "district","");
			    name=tranwrd(name, "hospital", "");
			    name=tranwrd(name, "hosp", "");
			    name=tranwrd(name, "ctr", "center");
			    name=tranwrd(name, "cntr", "center");
			    name=tranwrd(name, "medical", "med");
			    name=tranwrd(name, "tx", "texas");
			    name=tranwrd(name, "branch", "br");
			    name=tranwrd(name, "county", "co");
			    name=tranwrd(name, "rehabilitation", "rehab");
			    name=compbl(name);
			    name=tranwrd(name, "university texas", "ut");
			    name=tranwrd(name, "east texas med center","etmc");
			    name=tranwrd(name, "select specialty","ssh");
			    * PAPER: must come after ETMC;
			    name=tranwrd(name, "med", "");
			    name=tranwrd(name, "center", "");
			    * &outvar=tranwrd(&outvar, "texas", "");
			    name=compbl(name);
			    if length(name)<5 then name=lowcase(fname);
			   ;
			    *if i>1 then _droprow=1;
			   _droprow=.;
			    output;
	    *end;
	    drop nfid; *i nphone fac1-fac3: vars not in infile;
run;


proc print data=wfid_iter2 (obs=10); title " ";
run;
proc contents data=wfid_iter2 (obs=10); title " ";
run;


data data.fid0;
set wfid_iter2;
run;
