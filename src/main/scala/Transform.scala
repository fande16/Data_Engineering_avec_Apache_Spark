import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, date_format, desc, format_number, from_unixtime, sum}

object Transform {

  /**
   * Cette fonction permet de nettoyer les données brutes.
   * @param df : Dataframe
   * @return Dataframe
   */

  def cleanData(df: DataFrame): DataFrame = {
    /*
    0. Traiter toutes les colonnes en date timestamp vers YYYY/MM/DD HH:MM SSS.
     */

    val firstDF = df.withColumn("event_previous_timestamp",
      from_unixtime(col("event_previous_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss"))
      .withColumn("event_timestamp",
        from_unixtime(col("event_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss"))
      .withColumn("user_first_touch_timestamp",
        from_unixtime(col("user_first_touch_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss"))
    /*
    1. Extraire les revenus d'achat pour chaque événement
      - Ajouter une nouvelle colonne nommée revenue en faisant l'extration de ecommerce.purchase_revenue_in_usd
     */

    val revenueDF = firstDF.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))

    /*
    2. Filtrer les événements dont le revenu n'est pas null
    */
    val purchasesDF = revenueDF.filter(col("revenue").isNotNull)


    /*
    3. Quels sont les types d'événements qui génèrent des revenus ?
      Trouvez des valeurs event_name uniques dans purchasesDF.
      Combien y a t-il de type d'evenement ?
     */

    val distinctDF = purchasesDF.select("event_name").distinct()
    // Compter le nombre de types d'événements uniques
    val eventCount = distinctDF.count()
    /*
     4. Supprimer la/les colonne(s9 inutile(s)
      - Supprimez event_name de purchasesDF.
      */

    val cleanDF = purchasesDF.drop("event_name")

    cleanDF

  }


  /**
   * Cette fonction permet de récuperer le revenu cumulé par source de traffic, par0 etat et par ville
   * @param df : Dataframe
   * @return Dataframe
   */



  def computeTrafficRevenue(df : DataFrame): DataFrame = {

    /*
    5. Revenus cumulés par source de trafic par état et par ville city
      - Obtenir la somme de revenue comme total_rev
      - Obtenir la moyenne de revenue comme avg_rev
     */

    val trafficDF = df.groupBy("traffic_source", "geo.state", "geo.city")
      .agg(
        sum("revenue").as("total_rev"),
        avg("revenue").as("avg_rev")
      )

    /*
    6. Recuperer les cinqs principales sources de trafic par revenu total

     */
    val topTrafficDF = trafficDF.orderBy(desc("total_rev")).limit(5)


    /*
    7. Limitez les colonnes de revenus à deux décimales pointés
      Modifier les colonnes avg_rev et total_rev pour les convertir en des nombres avec deux décimales pointés
     */

    val finalDF = topTrafficDF
      .withColumn("total_rev", format_number(col("total_rev"), 2))
      .withColumn("avg_rev", format_number(col("avg_rev"), 2))


    finalDF


  }



}
