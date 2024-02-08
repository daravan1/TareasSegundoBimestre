import doobie.*
import doobie.implicits.*
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.awtrenderer.*
import org.saddle.{Index, Series, Vec}
import java.io.File



case class Tournament(tournaments_host_country: String)
case class ParticipationCount(participations: Int)

object Parte022 {
  @main
  def insfunc2(): Unit = {
    val path2DataFile: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    val path2DataFile2: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()
    reader2.close()

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/ejemplo",
      user = "root",
      password = "0000",
      logHandler = None
    )

    generateDataGoals(contentFile)


      def generateDataGoals(data: List[Map[String, String]]) =

        val sqlFormat = s"INSERT INTO goals(goals_goal_id,goals_match_id,goals_player_id,goals_minute_label,goals_minute_regulation,goals_minute_stoppage,goals_match_period,goals_own_goal,goals_penalty)" +
          s"VALUES('%s','%s', '%s', '%s', %d, %d, '%s', %d, %d);"

        val info = data
          .filterNot(_("goals_goal_id") == "NA")
          .map(row => (
            row("goals_goal_id"),
            row("matches_match_id"),
            row("goals_player_id"),
            row("goals_minute_label").replaceAll("'", "\\\\'"),
            row("goals_minute_regulation").toInt,
            row("goals_minute_stoppage").toInt,
            row("goals_match_period"),
            row("goals_own_goal").toInt,
            row("goals_penalty").toInt
          ))
          .distinct
          .sorted
          .map(x => sqlFormat.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))

          info.foreach(println)

    //Consultas
    //Paises que tiene el menor numero de participaciones como anfitriones
    def menorTorneos: ConnectionIO[ List[Tournament]] =
      sql"""
          SELECT tournaments_host_country
          FROM tournaments
          GROUP BY tournaments_host_country
          ORDER BY COUNT(*) ASC
          LIMIT 25;
        """
        .query[String]
        .to[List]
        .map(_.map(Tournament))


    val tournaments: List[Tournament] = menorTorneos.transact(xa).unsafeRunSync()
    println(s"Países con el menor número de torneos como anfitrión:")
    tournaments.foreach(tournament => println(tournament.tournaments_host_country))

    //Promedio total de goles de tods los partidos
    def calcularPromedioGoles: ConnectionIO[Double] =
      sql"""
        SELECT AVG(total_score) AS total_average
        FROM (
          SELECT SUM(score) AS total_score
          FROM (
            SELECT matches_home_team_score AS score FROM matches
            UNION ALL
            SELECT matches_away_team_score AS score FROM matches
          ) AS scores
        ) AS total_scores
      """
        .query[Double]
        .unique

    val totalAverage: Double = calcularPromedioGoles.transact(xa).unsafeRunSync()
    println(s"Promedio de goles: $totalAverage")

    //Numero de particpaciones de urugay en torneos
    def participacionesUruguay: ConnectionIO[ParticipationCount] =
      sql"""
        SELECT COUNT(*) AS participations
        FROM matches
        JOIN teams AS home_team ON matches.matches_home_team_id = home_team.team_id
        JOIN teams AS away_team ON matches.matches_away_team_id = away_team.team_id
        WHERE home_team.team_name = 'Uruguay' OR away_team.team_name = 'Uruguay'
      """
        .query[Int]
        .unique
        .map(count => ParticipationCount(count))

    val participations: ParticipationCount = participacionesUruguay.transact(xa).unsafeRunSync()
    println(s"Número de participaciones en partidos de Uruguay en todos los torneos: ${participations.participations}")


    //Grafica numero de partidos por region
    val queryConsultaRegionesMasUsadas = sql"""
      SELECT t.region_name, COUNT(*) AS num_matches
      FROM matches m
      INNER JOIN teams t ON m.matches_home_team_id = t.team_id OR m.matches_away_team_id = t.team_id
      GROUP BY t.region_name
      ORDER BY num_matches DESC
      LIMIT 6;
    """.query[(String, Int)]

    val dataConsultaRegiones: List[(String, Int)] = queryConsultaRegionesMasUsadas.to[List].transact(xa).unsafeRunSync()

    def charBarPlotRegiones(data: List[(String, Int)]): Unit = {
      val data4Chart: List[(String, Double)] = data.map(t2 => (t2._1, t2._2.toDouble))
      val indices = Index(data4Chart.map(_._1).toArray)
      val values = Vec(data4Chart.map(_._2).toArray)
      val series = Series(indices, values)
      val bar1 = saddle.barplotHorizontal(series, xLabFontSize = Option(RelFontSize(1)), color = RedBlue(70, 171))(par.xLabelRotation(-77).xNumTicks(0).xlab("Regiones").ylab("Partidos").main("Numero de partidos por Region"))
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\Regiones.png"), bar1.build, 1000)
    }
    charBarPlotRegiones(dataConsultaRegiones)

    //Estadios con mayor partidos jugados
    val queryConsultaPaisesEstadiosMasUsados = sql"""
      SELECT s.stadiums_stadium_name, COUNT(*) AS num_matches
      FROM matches m
      INNER JOIN stadiums s ON m.matches_stadium_id = s.matches_stadium_id
      GROUP BY s.stadiums_stadium_name
      ORDER BY num_matches DESC
      LIMIT 6;
    """.query[(String, Int)]

    val dataConsultaPaisesEstadios: List[(String, Int)] = queryConsultaPaisesEstadiosMasUsados.to[List].transact(xa).unsafeRunSync()

    def charBarPlotPaises(data: List[(String, Int)]): Unit = {
      val data4Chart: List[(String, Double)] = data.map(t2 => (t2._1, t2._2.toDouble))
      val indices = Index(data4Chart.map(_._1).toArray)
      val values = Vec(data4Chart.map(_._2).toArray)
      val series = Series(indices, values)
      val bar1 = saddle.barplotHorizontal(series, xLabFontSize = Option(RelFontSize(1)), color = RedBlue(70, 171))(par.xLabelRotation(-77).xNumTicks(0).xlab("Estadios").ylab("Numero de Partidos").main("Numero de Partidos por Estadios"))
      pngToFile(new File("C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\EstadiosPaises.png"), bar1.build, 1000)
    }
    charBarPlotPaises(dataConsultaPaisesEstadios)

  }
}
