# JSON Dateien in HDInsight verarbeiten

Ich werde häufige gefragt, wie man denn in HDInsight JSON Dateien verarbeiten kann. Es gibt mehrere Möglichkeiten, nachfolgended beschreibe ich, sowohl die Verarbeitung über Hive als auch über Spark.



Die JSON Dateien sehen wie folgt aus:

```

{"coord":{"lon":8.68,"lat":50.12},"sys":{"type":1,"id":4881,"message":0.0067,"country":"DE","sunrise":1491713071,"sunset":1491761404},"weather":[{"id":741,"main":"Fog","description":"fog","icon":"50n"}],"main":{"temp":7,"pressure":1024,"humidity":87,"temp_min":7,"temp_max":7},"visibility":10000,"wind":{"speed":1.5,"deg":70},"clouds":{"all":0},"dt":1491698826,"id":2925533,"name":"Frankfurt am Main"}

```



und sind in einem Azure Data Lake Store partitioniert über das Ingest Date abgelegt:



![JSON-HDInsight2](https://raw.githubusercontent.com/hau-mal/articles/master/images/JSON-HDInsight2.PNG)



## Verarbeitung mit Hive

Eine gute Beschreibung zur Verarbeitung von JSON-Dateien mit Hive in HDInsight findet ihr [hier](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-json-in-hive). Ich persönlich bevorzuge die Verwendung eines sogenannten Custom [SerDe](https://cwiki.apache.org/confluence/display/Hive/SerDe)s. SerDe ist die Abkürzung für Serializer/Deserializer, die die Verarbeitung von beliebigen Dateiformaten ermöglichen. Da JSON SerDes nicht mit Hive und leider auch noch nicht mit HDInsight per Default bereitgestellt werden, muss man dies selbst durchführen. Am Besten über ein sogenanntes Custom-Skript bei der Erstellung des Clusters.



Das SerDe stellst du mittels eines JAVA Archivs bereit. Folge einfach der Anleitung in der [HDInsight Dokumentation](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-json-in-hive) für die Erstellung des Archivs. Alternativ kannst du das JAR von meinem [GitHub](https://github.com/hau-mal/BigData/blob/master/jars/hive/json-serde-1.1.9.9-Hive1.2-jar-with-dependencies.jar) verwenden (habe es selbst mit HDI 3.6 verwendet, ist natürlich ohne jede Gewähr!).



Das Archiv musst du jetzt per Skript Aktion bei der Cluster-Erstellung hinzufügen. Befolge hierfür die Schritte unter [Add Custom Hive libraries when creating yout HDInsight cluster](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-add-hive-libraries).



Falls du deinen Cluster über ein ARM-Template erstellst, kannst du die Sript-Action auch analog der folgenden Abbildung einfügen:



![JSON-HDInsight1](https://raw.githubusercontent.com/hau-mal/articles/master/images/JSON-HDInsight1.PNG)



Für die JSON Datei ist dann eine Hive Tabelle anzulegen. Das folgene [Skript](https://github.com/hau-mal/BigData/blob/master/hql/et_weather.hql) beschreibt die Tabelle:



```

CREATE EXTERNAL TABLE  et_weather (
  coord STRUCT<lon:DOUBLE, lat:DOUBLE> COMMENT 'City geo location longitude and City geo location latitude',
  sys STRUCT <type:INT, id:INT, message:DOUBLE, country:STRING, sunrise:BIGINT, sunset:BIGINT> ,
  unix, UTC sys.sunset Sunset time unix UTC',
  weather ARRAY<STRUCT <id:BIGINT, main:STRING, description:STRING, icon:STRING>>,
  main STRUCT <temp:DOUBLE, humidity:DOUBLE, temp_min:DOUBLE, temp_max:DOUBLE> ,
  visibility BIGINT COMMENT 'Visibility meter' ,
  wind STRUCT <speed:DOUBLE, deg:DOUBLE>,  
  clouds STRUCT <a:double> COMMENT 'Cloudiness Percent',
  dt BIGINT COMMENT 'Time of data calculation Unix UTC', 
  id BIGINT COMMENT 'City ID',
  name STRING COMMENT 'City Name'
) 
COMMENT 'Current weather, see https://openweathermap.org/current for more details'
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 'adl://hmaneadls.azuredatalakestore.net/tenant01/data/raw/external/weather' 
;

```



Jetzt läßt sich die Tabelle abfragen, z. B.:

```

SELECT FROM_UNIXTIME(dt), main.temp, main.humidity
FROM et_weather
WHERE name = 'Seattle'
LIMIT 10;

```



liefert folgendes Ergebnis:



![JSON-HDInsight5](https://raw.githubusercontent.com/hau-mal/articles/master/images/JSON-HDInsight5.PNG)







## Spark Jupyter Notebook

Noch viel einfacher als mit Hive, lassen sich JSON-Dateien mit Spark lesen. Dies liegt darin, dass Spark JSON Dateien nativ unterstützt. Mittels read.json lassen sich JSON Daten lesen, es werden sogar automatisch die Partitionen erkannt:

![JSON-HDInsight3](https://raw.githubusercontent.com/hau-mal/articles/master/images/JSON-HDInsight3.PNG)



Die Notebooks lievern natürlich eine Menge weitere Funktionen. Hier wird der Temperaturverlauf grafisch ausgegeben. Die Daten wurden vorher in einer Parquet-Tabelle gespeichert.



![JSON-HDInsight4](https://raw.githubusercontent.com/hau-mal/articles/master/images/JSON-HDInsight4.PNG)



Hier findest du das komplette [Notebook](https://raw.githubusercontent.com/hau-mal/BigData/master/ipynb/WeatherSQL.ipynb).



