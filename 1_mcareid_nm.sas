
%include '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\config.sas'; 
*update above line with appropritate directory information;


data mcareid0;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile '\\pn-smb\proj1\pinfo_hck\karim\orglink_su2020\orglink_karim\replicate_orglink_karim_2018\raw\HOSPITAL10_PROVIDER_ID_INFO.CSV' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
	length fname $36 addr $36 city $27;	

		informat PROVIDER_NUMBER best32. ;
         informat FYB DATE8. ;
         informat FYE DATE9. ;
         informat STATUS $12. ;
         informat CTRL_TYPE best32. ;
         informat hosp_Name $36. ;
         informat Street_Addr $36. ;
         informat Po_Box $19. ;
         informat City $27. ;
         informat State $2. ;
         informat Zip_Code $10. ;
         informat County $10. ;

		format PROVIDER_NUMBER best12. ;
         format FYB DATE8. ;
         format FYE DATE9. ;
         format STATUS $12. ;
         format CTRL_TYPE best12. ;
         format hosp_Name $36. ;
         format Street_Addr $36. ;
         format Po_Box $19. ;
         format City $27. ;
         format State $2. ;
         format Zip_Code $10. ;
         format County $10. ;

		
      input
                  PROVIDER_NUMBER
                  FYB
                  FYE
                  STATUS $
                  CTRL_TYPE
                  hosp_Name $
                  Street_Addr $
                  Po_Box $
                  City $
                  State $
                  Zip_Code $
                  County $
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;



proc print data=mcareid0 (obs=10);
run;


 data data.mcareid0 (drop=state);
	set mcareid0 (keep=provider_number hosp_Name Street_addr city Zip_code state);

	where State='TX';
	rename provider_number=mcareid Street_addr=addr hosp_Name=hname Zip_code=zip; 

run;

proc print data=data.mcareid0 (obs=10); title " "; run;	

	

data data.mcareid0(drop=nmcareid);
    set data.mcareid0(keep=mcareid city addr hname zip rename=( mcareid=nmcareid hname=fname ));

label fname='hosp_Name' addr='Street_Addr' city='City' zip='Zip_Code';
	format _tmpvar $256.;
    format _src _des $256.;
    _src=compbl(fname);
    _des=_src;
    _len=0;
    do _i=1 to length(_src);
    if ('0'<=substr(_src,_i, 1)<='9' or 'A'<=substr(_src,_i, 1)<='Z' or 'a'<=substr(_src,_i, 1)<='z' or 
substr(_src,_i, 1)=' ') then do;
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
    if nmcareid=450770 then name='centraltexas';
    ******** make all vars char & standardize vars;
	length zip $10;
	format zip $5.;
	c_zip=compress(zip);
	zip=substr(c_zip, 1, 5);
    if ~(0<zip<=99999) then zip='';
   ;
    format _tmpvar $256.;
    format _src _des $256.;
    _src=compbl(addr);
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
    format mcareid $6.;
    mcareid=compress(nmcareid);
    ******** set as appropriate or keep for mult rows;
    _droprow=.; *assigns default value of '.' to _droprow, will be changed in later files to '1' if duplicate row;


run;

proc print data=data.mcareid0 (obs=10); title " "; run;	

