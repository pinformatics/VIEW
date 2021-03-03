# VIEW

* this is the initial attempt to merge fid and mcareid for 2018 data 
* sas code needs review and fine tunig before use.
* working directory information needs to be changed in address lines
* macros were not used till the first pass, but can be used
* macro library can be generated with the code
* data cleaning code in the sas files have been omited
* for the final manual review step following coding was used (var: mtch):
	1. match: 1
	2. probable match: 100
	3. multiple medicare: -1
	4. no match: blank
* see workflow file (based on 2017 data) for detailed description of how the program works
