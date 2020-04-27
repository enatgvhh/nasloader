# -*- coding: UTF-8 -*-
#LoaderAtkisNasForGn_40.py
from lxml import etree
import os
from nasloader4 import configloader
from nasloader4 import nasloader
    
def getConfigLoader(eList):
    confLoader = None
    
    for item in eList:
        strItem = etree.tostring(item, encoding='unicode')
        node = etree.fromstring(strItem)
    
        sourcefile = node.xpath('//sourcefile', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        logfile = node.xpath('//logfile', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        dbname = node.xpath('//dbname', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        user = node.xpath('//user', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        host = node.xpath('//host', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        port = node.xpath('//port', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        password = node.xpath('//password', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        schema = node.xpath('//schema', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        sourceurl = node.xpath('//sourceurl', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
        desturl = node.xpath('//desturl', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})[0].text
    
        confLoader = configloader.ConfigGmlLoader(logfile, dbname, user, host, port, password, schema, sourceurl, desturl)
        break
    
    return [confLoader,sourcefile]

def main():
    etConf = etree.parse('ConfigLoaderAlkis.xml')
    eListConf = etConf.xpath('//ConfigObject', namespaces={'xsi': 'http://www.w3.org/2001/XMLSchema-instance'})

    confLoaderList = getConfigLoader(eListConf)
    confLoader = confLoaderList[0]
    sourcefolder = confLoaderList[1]
    logger = confLoader.getLogger()
    
    featureTypes = [0]#for all types
    #featureTypes = [49,118,36]

    logger.info('Start')
    nasLoader = nasloader.NasLoader(logger,confLoader.getDatabaseConnection(),confLoader.getDatabaseSchema(),confLoader.getSourceCoordinate(),confLoader.getDestCoordinate())
    nasLoader.deleteDatabase()
    nasLoader.closeConnection()
    logger.info('delete database successfully')
    
    path = sourcefolder
    if os.path.exists(path) == True:
        if os.path.isdir(path) == True:
            objects = os.listdir(path)
            if objects:
                for objectElement in objects:
                    element = os.path.join(path, objectElement)
                    nasLoader = nasloader.NasLoader(logger,confLoader.getDatabaseConnection(),confLoader.getDatabaseSchema(),confLoader.getSourceCoordinate(),confLoader.getDestCoordinate())
                    et = etree.parse(element)
                    eList = et.xpath('//gml:featureMember/*', namespaces={'gml': 'http://www.opengis.net/gml/3.2','xlink': 'http://www.w3.org/1999/xlink', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'tn-ro': 'http://inspire.ec.europa.eu/schemas/tn-ro/4.0', 'net': 'http://inspire.ec.europa.eu/schemas/net/4.0', 'base': 'http://inspire.ec.europa.eu/schemas/base/3.3', 'tn': 'http://inspire.ec.europa.eu/schemas/tn/4.0', 'gco': 'http://www.isotc211.org/2005/gco','gmd': 'http://www.isotc211.org/2005/gmd','gn': 'http://inspire.ec.europa.eu/schemas/gn/4.0','o2i': 'http://list.smwa.sachsen.de/o2i/1.0'})
                    
                    succes = True
                    for item in eList:
                        if succes == True:
                            succes = nasLoader.loadNas(etree.tostring(item, encoding='unicode'), objectElement, featureTypes)
                        else:
                            break
                    
                    if succes == True:    
                        nasLoader.commitTransaction()
                        nasLoader.closeConnection()
                        logger.info('load database from ' + objectElement + ' successfully')
                    else:
                        nasLoader.closeConnection()
                        logger.warning('load database from ' + objectElement + ' terminated')
                    
    nasLoader.vacuumDatabase()
    logger.info('vacuum database successfully')  
    logger.info('End')
    
if __name__ == '__main__':
    main()
    