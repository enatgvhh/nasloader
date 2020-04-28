# -*- coding: UTF-8 -*-
#nasloader.py
import sys
import psycopg2
import re
from lxml import etree

class NasLoader(object):
    """Class NasLoader zum Laden von GML-FeatureMembers in einen deegree BLOB FeatureStore."""
    
    def __init__(self, logger, dbConnection, dbSchema, sourceEpsg, descEpsg):
        """Konstruktor der Klasse NasLoader.
        
        Konstruktor baut eine PostgreSQL-Connection zur Datenbank auf.
        Liest von dort die 'ft_types' aus der Tabelle 'feature_types' ein und legt damit ein Dictionary an.
        #Definiert ein Array mit den moeglichen Geometriearten z.B. 'gml:LineString'.
        
        Args:
            logger: Objekt logging.Logger
            dbConnection: String mit Datenbank-Connection
            dbSchema: String mit Datenbank-Schema
            sourceEpsg: String mit EPSG-Notation im GML-File, z.B. 'http://www.opengis.net/def/crs/EPSG/0/'
            descEpsg: String mit EPSG-Notation fuer BLOB-FeatureStrore, zwingend: 'EPSG:'
        """
        
        self.__logger = logger
        self.__strConnection = dbConnection
        self.__dbSchema = dbSchema
        self.__sourceEpsg = sourceEpsg
        self.__descEpsg = descEpsg
        self.conn = None
        self.cur = None
        self.__codeList = {}
        
        try:
            self.conn = psycopg2.connect(self.__strConnection)
            self.cur = self.conn.cursor()
            strSql = "SELECT * FROM " + self.__dbSchema + ".feature_types"
            self.cur.execute(strSql)
            
            #Dictionary ft_type    
            rows = self.cur.fetchall()
            for row in rows:
                strTypeList = row[1].split("}")
                strType = strTypeList[1]
                self.__codeList.update({strType: row[0]})
        except:
            message = "connection failed: " + str(sys.exc_info()[0]) + "; " + str(sys.exc_info()[1])
            self.__logger.error(message)
            self.closeConnection()
            sys.exit()
            
    def commitTransaction(self):
        """Methode speichert die Inserts in der Datenbank."""
        
        self.conn.commit()
        
    def closeConnection(self):
        """Methode schliesst die Datenbankverbindung."""
        
        self.cur.close()
        self.conn.close()
               
    def loadNas(self, element, filename, featureTypes):
        """Methode erzeugt aus dem Parameter 'element' ein SQL-Statement und setzt es gegen die Datenbank ab.
        
        Args:
            element: String mit dem GML-FeatureMember z.B. '<AX_Bundesland>...</AX_Bundesland>'
        """
        
        node = etree.fromstring(element.replace(self.__sourceEpsg,self.__descEpsg))
		
        geomList = node.xpath("//*[local-name()='position']")
        nameList = node.xpath("/*/*[local-name()='name']")
        name = None
        
        if nameList:
            name = nameList[0].text.lstrip(" ")
        
        if geomList and name and name != "" and not re.search('[0-9]+', name) and not re.match('\n', name):         
            gmlIdList = node.xpath('/*/@gml:id', namespaces={'gml': 'http://www.opengis.net/gml/3.2','xlink': 'http://www.w3.org/1999/xlink', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'tn-ro': 'http://inspire.ec.europa.eu/schemas/tn-ro/4.0', 'net': 'http://inspire.ec.europa.eu/schemas/net/4.0', 'base': 'http://inspire.ec.europa.eu/schemas/base/3.3', 'tn': 'http://inspire.ec.europa.eu/schemas/tn/4.0', 'gco': 'http://www.isotc211.org/2005/gco','gmd': 'http://www.isotc211.org/2005/gmd','gn': 'http://inspire.ec.europa.eu/schemas/gn/4.0','o2i': 'http://list.smwa.sachsen.de/o2i/1.0'})    
            featureTypeList = node.xpath('/*', namespaces={'gml': 'http://www.opengis.net/gml/3.2','xlink': 'http://www.w3.org/1999/xlink', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'tn-ro': 'http://inspire.ec.europa.eu/schemas/tn-ro/4.0', 'net': 'http://inspire.ec.europa.eu/schemas/net/4.0', 'base': 'http://inspire.ec.europa.eu/schemas/base/3.3', 'tn': 'http://inspire.ec.europa.eu/schemas/tn/4.0', 'gco': 'http://www.isotc211.org/2005/gco','gmd': 'http://www.isotc211.org/2005/gmd','gn': 'http://inspire.ec.europa.eu/schemas/gn/4.0','o2i': 'http://list.smwa.sachsen.de/o2i/1.0'})    

            gmlId = gmlIdList[0]
            strType = featureTypeList[0].tag.split("}")[1]
            intType = None
            for dictKeyType, dictValueType in self.__codeList.items():
                if strType == dictKeyType:
                    intType = dictValueType
                    break
        
            #nur Insert, wenn vorgegebener ft_type
            for featureType in featureTypes:
                if featureType == intType or featureType == 0:
                    strBinary = etree.tostring(node, encoding='unicode')
                    #xml minify
                    strBinary = re.sub('\n\s*', '', strBinary)
                    #for SQL Insert-Error
                    strBinary = re.sub("'", "''", strBinary)
                    strBinary = re.sub(r'\\', '/', strBinary)
                    
                    strSql = "INSERT INTO " + self.__dbSchema + ".gml_objects (gml_id,ft_type,binary_object) VALUES ('" + gmlId + "'," + str(intType) + ",'" + strBinary + "')"
                    
                    try:                
                        self.cur.execute(strSql)
                        return(True)
            
                    except:
                        message = "execute " + filename + " - " + gmlId + " failed: " + str(sys.exc_info()[0]) + "; " + str(sys.exc_info()[1]).replace("\n","")
                        self.__logger.error(message)
                        self.conn.rollback()
                        return(False)                   
                        
                    break
        else:
            return(True)
               
    def deleteDatabase(self):
        """Methode leert die Datenbank und setzt den Index zurueck"""
        
        try:            
            strSqlDel = "DELETE FROM " + self.__dbSchema + ".gml_objects"
            strSqlSeq = "ALTER SEQUENCE " + self.__dbSchema + ".gml_objects_id_seq RESTART WITH 1"
            self.cur.execute(strSqlDel)
            self.cur.execute(strSqlSeq)
            self.conn.commit()    
        except:
            message = "db delete failed: " + str(sys.exc_info()[0]) + "; " + str(sys.exc_info()[1])
            self.__logger.error(message)
            self.conn.rollback()
            self.closeConnection()
            sys.exit()
                   
    def vacuumDatabase(self):
        """Methode fuehrt Vacuum und Analyse auf Datenbank aus"""
        
        try:           
            conn = psycopg2.connect(self.__strConnection)
            conn.set_isolation_level(0)#0 = autocommit
            cur = conn.cursor()
            strSql = "VACUUM VERBOSE ANALYZE " + self.__dbSchema + ".gml_objects"
            cur.execute(strSql)        
        except:
            message = "db vacuum failed: " + str(sys.exc_info()[0]) + "; " + str(sys.exc_info()[1])
            self.__logger.error(message)
            sys.exit()
        finally:
            cur.close()
            conn.close()
            