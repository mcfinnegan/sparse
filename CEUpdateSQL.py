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

conn_string = 'data source=XP-STEVE\SQL2008_Steve; initial catalog=eMedPreV3; trusted_connection=True;MultipleActiveResultSets=true;'
connection = SqlConnection(conn_string)
connection.Open()

s = StreamWriter('CEupdateSQL.txt')

tablename = "ClinicalEvent"

#SET IDENTITY_INSERT [ClinicalEvent] ON
#
#INSERT
#	[ClinicalEvent]
#	(
#		ClinicalEventID,
#		ClinicalEventTypeID,
#		PatID,
#		ClinicalEventStartDate
#	)
#SELECT 
#		ClinicalEventID,
#		ClinicalEventTypeID,
#		PatID,
#		Date
#FROM 
#	eMedPreV3.dbo.tblClinicalEvent
#
#SET IDENTITY_INSERT [ClinicalEvent] OFF




cmd3 = SqlCommand ("select ClinicalDataType, ClinicalDataTypeID, FileIniID from tblClinicalDataType", connection)
rdr2 = cmd3.ExecuteReader()

while rdr2.Read():

 	
	fi = rdr2["FileIniID"]
	cd = "[" + rdr2["ClinicalDataType"].replace(" ","_").replace("/","_").replace("-","_") + "]"
	cdid = rdr2["ClinicalDataTypeID"]


	if fi==77:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  Text FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalDataText T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 77  AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==79:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  D.ClinicalDataID FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.TBLCLINICALdatacheckvalue T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 79 AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==82:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  NoteID FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalDataNote N ON N.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataCustomCode T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 82 AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==88:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  CustomCodeID FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalDataCustomCode T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 88  AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==89:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  ReadInt FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalDataReadInt T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 89 AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==120:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  CAST(Text AS FLOAT) FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalText T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 120 AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	elif fi==121:	
		s.Write(String.Format("UPDATE [{2}] SET {0} =  CAST(Text AS DateTime) FROM [{2}] INNER JOIN eMedPreV3.dbo.tblClinicalData D ON [{2}].ClinicalEventID=D.ClinicalEventID INNER JOIN eMedPreV3.dbo.tblClinicalText T ON T.ClinicalDataID=D.ClinicalDataID INNER JOIN eMedPreV3.dbo.tblClinicalDataType DT ON DT.ClinicalDataTypeID = D.ClinicalDataTypeID WHERE FileIniID = 120 AND D.ClinicalDataTypeID = {1}\r\n", cd, cdid, tablename))
	else:
		s.Write("")

s.Close()
rdr2.Close()