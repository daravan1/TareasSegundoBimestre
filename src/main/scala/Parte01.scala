import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.awtrenderer.*
import org.nspl.data.HistogramData
import org.saddle.{Index, Series, Vec}
import java.io.File

implicit object CustomFormat extends DefaultCSVFormat{
  override val delimiter: Char = ';'
}
object Parte01 {
  def main(args: Array[String]): Unit = {
    println("Demo")
    val path2DataFile: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()

    val path2DataFile2: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()


    //Funciones en base a los archivos
    promedioPuntuacion(contentFile)
    MenorTorneos(contentFile)
    MaxMujeres(contentFile)

    //Graficos
    charting01(contentFile)
    chartBarPlot(contentFile)
    ganadorBrazil(contentFile)
    numeroCamisas(contentFile2)
    puntajeLocal(contentFile)
    estadioCap(contentFile)



    //Funcion que devuelve el promedio de los goles
    def promedioPuntuacion(data: List[Map[String, String]]): Unit = {
      val homeScoreIndex = data.head.getOrElse("matches_home_team_score", "").toInt
      val awayScoreIndex = data.head.getOrElse("matches_away_team_score", "").toInt

      val homeScores = data.map(row => row.getOrElse("matches_home_team_score", "").toInt)
      val awayScores = data.map(row => row.getOrElse("matches_away_team_score", "").toInt)
      val totalScores = homeScores ++ awayScores
      val totalAverage = totalScores.sum.toDouble / totalScores.length
      println(s"Promedio de goles ${totalAverage}")
      }
    
    //Funcion que devuelve el pais con el menor numero de torneos como anfitricion
    def MenorTorneos(data: List[Map[String, String]]): Unit = {
      val contadorTorneosPorPais = data.groupBy(_("tournaments_host_country")).view.mapValues(_.size)
      val paisMenorTorneos = contadorTorneosPorPais.minBy(_._2)._1
      println(s"País con el menor número de torneos anfitrion: ${paisMenorTorneos}")
    }

    //Funcion que devuelve el pais con el mayor numero de equipos femeninos locales e invitados
    def MaxMujeres(data: List[Map[String, String]]): Unit = {
      val MujeresLocal = data.map(row => (row("home_team_name"), row("home_womens_team").toInt)) //extrae datos de dos atributos
      val MaxMujeresLocal = MujeresLocal.maxBy(_._2)._1

      val MujeresInvitado = data.map(row => (row("away_team_name"), row("away_womens_team").toInt))
      val MaxMujeresInvitado = MujeresInvitado.maxBy(_._2)._1

      println(s"País con el mayor número de equipos femeninos locales: ${MaxMujeresLocal}")
      println(s"País con el mayor número de equipos femeninos invitados: ${MaxMujeresInvitado}")
    }

    //Frecuencia de numero de equipos por torneo
    def charting01(data: List[Map[String, String]]): Unit = {
      val listTeams: List[Double] = data
        .filter(row => row("tournaments_count_teams") != "NA")
        .map(row => row("tournaments_count_teams").toDouble)

      val histTeams = xyplot(HistogramData(listTeams, 20) -> bar())(
        par
          .xlab("Numero de Equipos")
          .ylab("Frecuencia en torneos")
          .main("Equipos x Torneos")
      )
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\equipos.png"), histTeams.build, 1000)
   }

    // Frecuencia de paises que han sido anfitriones
    def chartBarPlot(data: List[Map[String, String]]): Unit = {
      val uniqueTournamentsByYear = data.map(_("tournaments_year")).distinct.flatMap { year =>
        data.find(_("tournaments_year") == year)
      }
      val countriesCount = uniqueTournamentsByYear.groupBy(_("tournaments_host_country")).view.mapValues(_.size).toList.sortBy(-_._2)
      val data4Chart: List[(String, Double)] = countriesCount.map { case (country, count) => (country.substring(0, 4), count.toDouble) }
      val indices = Index(data4Chart.map { case (label, _) => label.substring(0, 3) }.toArray)
      val values = Vec(data4Chart.map { case (_, value) => value }.toArray)

      val series = Series(indices, values)

      val bar1 = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(1)),
        color = RedBlue(86, 146))(
        par
          .xLabelRotation(-77)
          .xNumTicks(0)
          .xlab("Paises")
          .ylab("freq.")
          .main("Torneos"))
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\MundialesHombre.png"), bar1.build, 1000)
      }

    def ganadorBrazil (data: List[Map[String, String]]): Unit = {
      val winBrazil: List[Double] = data
        .filter(row => row("tournaments_winner") == "Brazil" && row("tournaments_year") != "0")
        .map(row => row("tournaments_year").toDouble)
      val grpoint = xyplot(winBrazil -> point())(
        par
          .xlab("Brazil ")
          .ylab("Años")
          .main("Años ganados por Brazil")
      )
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\Brazil .png"), grpoint.build, 1000)
      renderToByteArray(grpoint.build, width = 2000)
    }
    // Frecuencia numero de camisas 
    def numeroCamisas(data: List[Map[String, String]]): Unit = {
      val camisas: List[Double] = data
        .filter(row => row("squads_shirt_number")  != "0")
        .map(row => row("squads_shirt_number").toDouble)
      val linea = xyplot(HistogramData(camisas, 20) -> line())(
        par
          .xlab("Numero de camiseta")
          .ylab("Frecuencia")
          .main("Frecuencia de numero de Camisetas")
      )
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\Camisas.png"), linea.build, 1000)
      renderToByteArray(linea.build, width = 2000)
    }

    //Puntajes locales
    def puntajeLocal(data: List[Map[String, String]]): Unit = {
      val ListaValores: List[Double] = data
        .filter(row =>
          row("matches_home_team_score") != "0")
        .map(row => row("matches_home_team_score").toDouble)

      val datos = xyplot(HistogramData(ListaValores, 20) -> bar())(
        par
          .xlab("x")
          .ylab("y")
          .main("Puntaje local")
      )

      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\ScoreLocal.png"), datos.build, width = 1000)
      renderToByteArray(datos.build, width = 2000)
    }

    //Capacidad de los estadios
    def estadioCap(data: List[Map[String, String]]): Unit = {
      val ListaValores: List[Double] = data
        .filter(row =>
          row("stadiums_stadium_capacity") != "0")
        .map(row => row("stadiums_stadium_capacity").toDouble)

      val datos = xyplot(ListaValores -> point())(
        par
          .xlab("")
          .ylab("")
          .main("Capacidad del estadio")
      )

      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\estadio.png"), datos.build, width = 1000)

      renderToByteArray(datos.build, width = 2000)
    }
  }
}
