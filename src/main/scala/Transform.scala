import org.apache.spark.sql.DataFrame

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

    val firstDF = df.FILL_IN

    /*
    1. Extraire les revenus d'achat pour chaque événement
      - Ajouter une nouvelle colonne nommée revenue en faisant l'extration de ecommerce.purchase_revenue_in_usd
     */

    val revenueDF = firstDF.FILL_IN

    /*
    2. Filtrer les événements dont le revenu n'est pas null
    */
    val purchasesDF = revenueDF.FILL_IN


    /*
    3. Quels sont les types d'événements qui génèrent des revenus ?
      Trouvez des valeurs event_name uniques dans purchasesDF.
      Combien y a t-il de type d'evenement ?
     */

    val distinctDF = purchasesDF.FILL_IN

    /*
     4. Supprimer la/les colonne(s9 inutile(s)
      - Supprimez event_name de purchasesDF.
      */

    val cleanDF = distinctDF.FILL_IN

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

    val trafficDF = df.FILL_IN

    /*
    6. Recuperer les cinqs principales sources de trafic par revenu total

     */
    val topTrafficDF = trafficDF.FILL_IN


    /*
    7. Limitez les colonnes de revenus à deux décimales pointés
      Modifier les colonnes avg_rev et total_rev pour les convertir en des nombres avec deux décimales pointés
     */

    val finalDF = topTrafficDF.FILL_IN

    finalDF


  }



}
