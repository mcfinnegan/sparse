import clr
import System
clr.AddReference('System.Data')
from System import *
from System.Data.SqlClient import *
from System.Data import *
from System.Text import *
from System.IO import *

clr.AddReference('System.Xml')
from System.Xml import *

import System.Text.RegularExpressions
from System.Text.RegularExpressions import *

from System.Diagnostics import *

conn_string = 'data source=XP-STEVE\SQL2005; initial catalog=eMedPre30; trusted_connection=True;MultipleActiveResultSets=true;'
connection = SqlConnection(conn_string)
connection.Open()

s = StreamWriter('CESQL.txt')

cmd3 = SqlCommand ("select ClinicalDataType, ClinicalDataTypeID, FileIniID from tblClinicalDataType", connection)
rdr2 = cmd3.ExecuteReader()

tablename = "ClinicalEvent"
s.WriteLine(String.Format("CREATE TABLE {0} (\r\n\r\nClinicalEventID int IDENTITY(1,1) NOT NULL,\r\nPatID int NOT NULL,\r\nClinicalEventTypeID int NOT NULL,\r\nEventTypeID as ClinicalEventTypeID,\r\nClinicalEventStartDate DateTime NOT NULL,\r\nClinicalEventEndDate DateTime NULL,\r\nAllData XML COLUMN_SET FOR ALL_SPARSE_COLUMNS,\r\n", tablename))
while rdr2.Read():

	cd = "[" + rdr2["ClinicalDataType"].Replace(" ","_").Replace("-","_").Replace("/","_") + "]"
	cdid = rdr2["ClinicalDataTypeID"]
	fid = rdr2["FileIniID"]

	s.Write(cd + " ")

	if fid==77:	
		s.Write("varchar(max)")
	elif fid==79:	
		s.Write("int")
	elif fid==82:	
		s.Write("int")
	elif fid==88:	
		s.Write("int")
	elif fid==120:	
		s.Write("float")
	elif fid==121:	
		s.Write("datetime")
	else:
		s.Write("int")

	s.WriteLine(" " + "SPARSE,");

s.WriteLine("\r\nTimeStamp DateTime NULL,\r\n");
s.WriteLine("CONSTRAINT [PK_ClinicalEvent] PRIMARY KEY CLUSTERED\r\n(\r\n[ClinicalEventID] ASC\r\n) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]\r\n) ON [PRIMARY]\r\n\r\n")
rdr2.Close()

rdr2 = cmd3.ExecuteReader()
while rdr2.Read():

	cd = "[" + rdr2["ClinicalDataType"].Replace(" ","_").Replace("-","_").Replace("/","_")  + "]"
	cdid = rdr2["ClinicalDataTypeID"].ToString()

	s.WriteLine(" EXEC sys.sp_addextendedproperty \r\n@name = N'DataTypeID',\r\n@value = N'{0}',\r\n@level0type = N'SCHEMA', @level0name = dbo,\r\n@level1type = N'TABLE',  @level1name = {2},\r\n@level2type = N'COLUMN', @level2name = {1};\r\nGO\r\n",cdid, cd, tablename)


rdr2.Close()


#rdr2 = cmd3.ExecuteReader()
#while rdr2.Read():
#
#	cd = "[" + rdr2["ClinicalDataType"] + "]"
#	cdid = rdr2["ClinicalDataTypeID"].ToString()
#
#	s.WriteLine("CREATE NONCLUSTERED INDEX INDEX_{0} ON dbo.SparseEvent({1}) WHERE {1} IS NOT NULL;\r\nGO\r\n", cdid, cd)
#
#
#rdr2.Close()


#rdr2 = cmd3.ExecuteReader()
#while rdr2.Read():
#
#	cdid = rdr2["ClinicalDataTypeID"].ToString()
#
#	s.WriteLine("DROP INDEX dbo.SparseEvent.INDEX_{0};\r\nGO\r\n", cdid)

#rdr2.Close()

connection.Close()
s.Close()


