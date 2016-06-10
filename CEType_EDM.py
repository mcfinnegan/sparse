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


cmd2 = SqlCommand ("select ClinicalEventType, ClinicalEventTypeID from tblClinicalEventType", connection)
rdr = cmd2.ExecuteReader()

modelName = "Events"
tableName = "Event"
basetypeName = "ClinicalEvent"

recs = []

while rdr.Read():
	
	ClinicalEventType = rdr["ClinicalEventType"].replace(" ","_").replace("-","_").replace("/","_")
	ClinicalEventTypeID = rdr["ClinicalEventTypeID"]

	typeName = ClinicalEventType


	recs.append((ClinicalEventTypeID, typeName))
	print typeName

rdr.Close()


wri=XmlTextWriter("ce_edm.xml", System.Text.Encoding.UTF8)
wri.WriteStartDocument()
wri.WriteStartElement("Mapping")


for c in recs:

	ClinicalEventTypeID = c[0]
	typeName= c[1].replace(" ","_").replace("-","_").replace("/","_")

	wri.WriteStartElement("EntityTypeMapping")
	wri.WriteAttributeString("TypeName", String.Format("IsTypeOf({0}.{1})", modelName, typeName))
	wri.WriteStartElement("MappingFragment")
	wri.WriteAttributeString("StoreEntitySet", tableName)


	cmd3 = SqlCommand ("select ClinicalDataType from tblClinicalEventTypeTemplate T inner join tblClinicalDataType D on T.ClinicalDataTypeID=D.ClinicalDataTypeID where  ClinicalEventTypeID=" + ClinicalEventTypeID.ToString(), connection)
	rdr2 = cmd3.ExecuteReader()

	while rdr2.Read():

		cdata_columnName = rdr2["ClinicalDataType"].replace(" ","_").replace("-","_").replace("/","_")
		cdata_propName = cdata_columnName.replace(" ","_").replace("-","_").replace("/","_")

		wri.WriteStartElement("ScalarProperty")
		wri.WriteAttributeString("Name", cdata_propName )
		wri.WriteAttributeString("ColumnName", cdata_columnName )
		wri.WriteEndElement()


	wri.WriteStartElement("Condition")
	wri.WriteAttributeString("ColumnName", "ClinicalEventTypeID" )
	wri.WriteAttributeString("Value", ClinicalEventTypeID.ToString() )
	wri.WriteEndElement()


        wri.WriteEndElement()    # </MappingFragment>
        wri.WriteEndElement()	 # </EntityTypeMapping>

wri.WriteStartElement("Mapping")

wri.Close()


wri=XmlTextWriter("ce_edm_2.xml", System.Text.Encoding.UTF8)
wri.WriteStartDocument()
wri.WriteStartElement("Conceptual")


for c in recs:

	ClinicalEventTypeID = c[0]
	typeName= c[1].replace(" ","_").replace("-","_").replace("/","_")

	wri.WriteStartElement("EntityType")
	wri.WriteAttributeString("Name", typeName)
	wri.WriteAttributeString("BaseType", String.Format("IsTypeOf({0}.{1})", modelName, basetypeName))

	cmd3 = SqlCommand ("select ClinicalDataType, D.FileIniID from tblClinicalEventTypeTemplate T inner join tblClinicalDataType D on T.ClinicalDataTypeID=D.ClinicalDataTypeID where  ClinicalEventTypeID=" + ClinicalEventTypeID.ToString(), connection)
	rdr2 = cmd3.ExecuteReader()

	while rdr2.Read():

		cdata_columnName = rdr2["ClinicalDataType"].replace(" ","_").replace("-","_").replace("/","_")
		cdata_propName = cdata_columnName.replace(" ","_").replace("-","_").replace("/","_")
		cdata_columnType = "Int32"

		fid = rdr2["FileIniID"]
		if fid==77:	
			cdata_columnType = "String"
		elif fid==79:	
			cdata_columnType = "Int32"
		elif fid==82:	
			cdata_columnType = "Int32"
		elif fid==88:	
			cdata_columnType = "Int32"
		elif fid==120:	
			cdata_columnType = "Double"
		elif fid==121:	
			cdata_columnType = "DateTime"
		else:
			cdata_columnType = "Int32"

		wri.WriteStartElement("Property")
		wri.WriteAttributeString("Type", cdata_columnType )
		wri.WriteAttributeString("Name", cdata_propName )
		wri.WriteEndElement()


        wri.WriteEndElement()	 # </EntityType>

wri.WriteEndElement()	   # </Conceptual>

wri.Close()


connection.Close()


