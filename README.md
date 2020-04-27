#

Simple NAS-File Loader - python multiprocessing
===============================================

## Inhalt
* [Einleitung](#einleitung)
* [deegree SQLFeatureStore](#deegree-sqlfeaturestore)
* [Python Multiprocessing](#python-multiprocessing)
* [FME Transformation](#fme-transformation)
* [Summary](#summary)


## Einleitung
Im Beitrag [GML-File Loader](https://github.com/enatgvhh/gmlloader) bin ich im letzten Kapitel bereits auf die Nachnutzung für NAS-Files eingegangen. Das python-package 'nasloader4' stimmt weitgehend mit dem package 'gmlloader4' überein, ist aber aufgrund der angeführten Punkte etwas leichter. Der Schwerpunkt dieses Beitrages soll deshalb mehr auf der praktischen Umsetzung, auf dem Multiprocessing sowie der Weiterverarbeitung mit der Feature Manipulation Engine (FME) liegen.

Das python-package 'nasloader4' und ein Client sind im Ordner [src](src) zu finden. Das Multiprocessing ist im Quellcode nicht enthalten, da es eine sehr individuelle Lösung ist. Ich werde es hier nur anhand von Codeschnipseln darstellen.


## deegree SQLFeatureStore
Den Inhalt der NAS-Files laden wir in eine PostgreSQL/PostGIS Datenbank (*deegree SQLFeatureStore im BLOB-Modus*). In diesem FeatureStore wird ein xml-Objekt einfach in einem BLOB-Attribut gespeichert. Das [Konfigurationsfile](config/nas_alkis.xml) für den deegree SQLFeatureStore kann mit dem 'deegree-cli-utility' Client bzw. dem neueren 'deegree-gml-tool' generiert werden. Genauso das [sql-File](config/nas_alkis.sql) zum Anlegen der Datenbanktabellen.


## Python Multiprocessing
Ziel der praktischen Umsetzung ist es, das Veränderungen der NAS-Files (*ALKIS, ATKIS*) selbständig erkannt werden. Bei einem Change, wird der Ladevorgang in die Datenbank sowie daran anschließend, eine FME Workbench, zur Transformation in das INSPIRE Zielschema, ausgeführt.

Um unsere Client-Funktionalität in einen Multiprozess zu verpacken, einen Prozess, der permanent läuft, machen wir daraus eine Klasse, in deren Konstruktor der super-Konstruktor aufgerufen und in der die *super-run()* Methode überschrieben wird.
```
class MultiprocessLoaderAlkis(multiprocessing.Process):
    """Class MultiprocessLoaderAlkis ist ein Prozess, der Aenderungen der Input-Files ueberwacht und
    bei einer festgestellten Aenderung den gesamten Ladevorgang startet.
    """

    def __init__ (self, path, hashFile):
        """Konstruktor der Klasse MultiprocessLoaderAlkis.
        
        Konstruktor ruft den super-Konstruktor auf.
        
        Args:
            path: String with path to input files
            hashFile: String with path/file to hash-sum-file
        """
        
        multiprocessing.Process.__init__(self)
        self.__path = path
        self.__hashFile = hashFile
        self.__BLOCKSIZE = 65536
        
    def run(self):
        """overwrite the super-run()"""
        
        while True:
            self.checkHash()
            time.sleep(86400)#1h = 3600 1d=86400
            
    def checkHash(self):
        """Methode checkt hash-sums..."""
		pass
```
Von dieser Klasse initialisieren wir dann ein Objekt und starten den Multiprozess, der nach getaner Arbeit für 24h in den Schlafmodus übergeht.
```
from myThreads import multiprocessLoaderAlkis

if __name__ == "__main__":
    """ Jeder process laeuft in einem eigenem Prozess neben dem main ab."""
    
    processAlkis = multiprocessLoaderAlkis.MultiprocessLoaderAlkis(r'E:\Data\ALKIS\ALKIS_HH_unzip', r'E:\logs\hash_alkis.txt')
    processAlkis.start()
```
Die Veränderung der NAS-Files erkennen wir über Hash-Summen, die wir ganz einfach in einem File abspeichern.
```
def checkHash(self):
        """Methode checkt hash-sums..."""
        
        hashDict = {}
        hashChanged = False
        
        if os.path.isfile(self.__hashFile):
            reader = open(self.__hashFile, "r")
        
            for line in reader:
                zeile = line.rstrip("\n").split("\t")
                hashDict.update({zeile[0]: zeile[2]})
        
            reader.close()
            os.remove(self.__hashFile)
            
        writer = open(self.__hashFile, "a")
           
        if os.path.exists(self.__path) == True:
            if os.path.isdir(self.__path) == True:
                objects = os.listdir(self.__path)
                if objects:
                    for objectElement in objects:
                        element = os.path.join(self.__path, objectElement)
                        hasher = hashlib.sha256()     
                        with open(element, 'rb') as afile:
                            buf = afile.read(self.__BLOCKSIZE)          
                            while len(buf) > 0:
                                hasher.update(buf)
                                buf = afile.read(self.__BLOCKSIZE)
                
                        newHash = hasher.hexdigest()
                        writer.write(element + "\tSHA256:\t" + newHash + "\n")
                        
                        for dictKeyType, dictValueType in hashDict.items():
                            if dictKeyType == element:
                                if newHash != dictValueType:
                                    hashChanged = True
                                    
                                break
        writer.close()
        if hashChanged == True:
            print("hash changed, start MultiprocessLoaderAlkis!")
            self.loader()
        else:
            print("hash not changed, sleep MultiprocessLoaderAlkis!")
```
Und die FME Workbench können wir über einen python-subprocess starten…


## FME Transformation
In der FME Workbench greifen wir mit einem PostGIS-Reader auf die Daten zu, decodieren den Inhalt des Attributes 'binary_object' (vgl. Abb. 1 und FME Custom Transformer [GML_Objects_BLOB_Decoder]( https://github.com/enatgvhh/inspire)) und extrahieren mit 'XMLXQueryExtractor' Transformern, über xpath/xquery-Ausdrücke, die benötigten Informationen. Gegenüber der SQL-Welt hat das den großen Vorteil, dass wir alle AAA-Objektarten generisch verarbeiten können.

![blob_decoder.jpg](img/blob_decoder.jpg)
Abb. 1: PostGIS-Reader und BLOB-Dekodierung

Produktiv verwende ich den ALKIS- und den ATKIS-Zwischenspeicher aber letztendlich nur für die Transformation in das INSPIRE Zielschema 'Geographical Names'. Ansonsten werden die AdV-Daten über die HALE [AdV INSPIRE alignments](https://github.com/enatgvhh/hale-adv) transformiert. Diese führten bei den 'Geographical Names' aber zu völlig unbrauchbaren Ergebnissen.


## Summary
Mit dem vorgestellten Ansatz habe ich eine Möglichkeit aufgezeigt, wie sich AAA-Daten auf recht unkomplizierte Weise für die INSPIRE Transformation verwenden lassen. Möchte man die AdV-Daten vollständig über diesen Workflow laufen lassen, sollte man den Sachverhalt noch etwas genauer evaluieren. Ich kenne mich mit dem AAA-Modell nämlich überhaupt nicht aus. Und an dem Code bedarf es dann entsprechender Änderungen, da jetzt zum Beispiel in der Klasse 'NasLoader' nur Objekte berücksichtigt werden, die über eine Geometrie und einen brauchbaren Wert im Attribut 'name' verfügen.

Die Grundidee hinter diesem Ansatz ist die, dass man nicht für jede Transformation immer alle NAS-Files durchlaufen muss, sondern gezielt die benötigten Objektarten selektiert. Auf diese Idee sind natürlich schon andere gekommen. So lassen sich mit [norGIS-ALKIS-Import](https://www.norbit.de/68/) ALKIS-Daten in eine PostgreSQL/PostGIS Datenbank laden. Für dieses Datenmodell gibt es dann wiederum entsprechende AdV INSPIRE alignments. Ansprechpartner hierfür ist die AdV. Über die Prozess-Funktionalität kann ich keine Aussage treffen.
