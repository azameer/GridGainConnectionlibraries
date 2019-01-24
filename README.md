# GridGainConnectionlibraries
This connection library (GGainDBMgr) enables us to connect to Grid Gain server and execute queries,
the following are the prerequisites & Usage to execute a query on Grid Gain.

PREREQUISITES: The following modules should be in same directory where the query execution script 
is executed.

GGDbMgr.pm
Logger.pm
GGainDBMgr.pm




USAGE: perl ExecuteGridGainQry.pl -opt=DBOperation -q=query string

options are INSERT|UPDATE|DELETE|DROP|CREATE|SELECT

Examples:       

perl ExecuteGridGainQry.pl -opt=Insert -q=\"Insert into TestTable values('test')\"

perl ExecuteGridGainQry.pl -opt=Select -q=\"Select * From TestTable\";

