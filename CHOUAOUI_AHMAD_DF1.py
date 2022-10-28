from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("ApplePrices")
parser.add_argument("CurrencyConversion")
args = parser.parse_args()
applePrices = args.ApplePrices
currencyConversion = args.CurrencyConversion

spark = SparkSession \
    .builder \
    .appName("Projet Apple products") \
    .getOrCreate()

applePricesDF = spark.read.format("csv").option("header",True).option("inferSchema",True).load(applePrices)
currencyConversionDF = spark.read.format("csv").option("header",True).option("inferSchema",True).load(currencyConversion)


## Exercice 1 : all apple product in USD ##
priceInUSD = applePricesDF.join(currencyConversionDF,applePricesDF.Currency == currencyConversionDF.ISO_4217,"inner")\
        .select("Model_name",(col("Price") / col("Dollar_to_Curr_Ratio")).alias("Country_Price_In_USD"),"Country") #On divise les prix par les ratios des pays et on renomme la colonne


## Exercice 2 : average Prices in countries ##
priceUSA = priceInUSD.where(col("Country") == "United States")\
        .withColumnRenamed("Country_Price_In_USD","USA_Prices").withColumnRenamed("Country", "USA") #On récupère les données pour les USA et on rename les colonnes sinon on a des doublons

joinPrices= priceInUSD.join(priceUSA,priceInUSD.Model_name==priceUSA.Model_name,"inner")#On joint le DataFrame des données des USA et le Dataframe de base sur le Model_name, ça permet d'avoir un DataFrameoù on pourra comparer les USA et tout les autres pays
averagePrices = joinPrices.groupBy("Country")\
        .agg(avg("Country_Price_In_USD").alias("Average_Price") , avg("USA_Prices").alias("Average_Price_USA"))\
        .withColumn("Difference", ((col("Average_Price")-col("Average_Price_USA"))/col("Average_Price_USA"))*100)\
        .orderBy(col("Average_Price").desc()) #On fait la moyenne des prix pour les X produit du pays et pareil pour les même x produit mais au USA et regroupe par pays pour avoir 1 ligne par pays, ensuite on fait le calcul d'écart et on met dans l'ordre décroissant 
averagePrices.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("moyennePrix.csv")



## Exercice 3 : all apple product cost in all contry ##
totalCost = priceInUSD.groupBy("Country").agg(sum("Country_Price_In_USD").alias("Country_Price_In_USD")) #On utilise l'aggrégation (.agg()) seulement pour pouvoir renommer la colonne des sommes avec un alias ça fait plus propre dans le retour
totalCost.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("coutTotal.csv")


## Exercice 4 : list all products ##
listProduct = priceInUSD.select("Model_name").distinct() #On ajoute le distinct pour ne pas avoir de doublons
listProduct.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("listeProduit.csv")


## Exercice 5 : find the cheapest airpods pro around the world ##
cheepestAirpodPro = priceInUSD.select("Model_name" , "Country", "Country_Price_In_USD")\
        .where(col("Model_name") == "AirPods Pro").sort(asc("Country_Price_In_USD"))#On prend les colonnes qu'on veut afficher avec comme modele les AirPods Pro et on ordonne par prix croissant
cheepestAirpodPro.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("airpodsPro.csv")
