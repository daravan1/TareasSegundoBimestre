import com.github.tototoshi.csv._

import java.io.File
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.implicits._

object Insercion {
  @main
  def insfunc(): Unit = {

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/ejemplo",
      user = "root",
      password = "0000",
      logHandler = None
    )

   
    val path2DataFile: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()

    val path2DataFile2: String = "C:\\Users\\darav\\Desktop\\ProyectoFinalFuncional\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()

    //metodo 1
    def dataForTeams(homeTeamData: List[Map[String, String]], awayTeamData: List[Map[String, String]]): List[Update0] = {
      val homeTeamUpdates = homeTeamData.map { home =>
        (
          home("matches_home_team_id"),
          home("home_team_name").replaceAll("'", "\\\\'"),
          home("home_mens_team").replaceAll("'", "\\\\'"),
          home("home_womens_team").replaceAll("'", "\\\\'"),
          home("home_region_name").replaceAll("'", "\\\\'")
        )
      }.map { team =>
        sql"""INSERT INTO teams(team_id, team_name, mens_team, womens_team, region_name)
             VALUES (${team._1}, ${team._2}, ${team._3}, ${team._4}, ${team._5})
             ON DUPLICATE KEY UPDATE
             team_name = VALUES(team_name),
             mens_team = VALUES(mens_team),
             womens_team = VALUES(womens_team),
             region_name = VALUES(region_name)""".update
      }

      val awayTeamUpdates = awayTeamData.map { away =>
        (
          away("matches_away_team_id"),
          away("away_team_name").replaceAll("'", "\\\\'"),
          away("away_mens_team").replaceAll("'", "\\\\'"),
          away("away_womens_team").replaceAll("'", "\\\\'"),
          away("away_region_name").replaceAll("'", "\\\\'")
        )
      }.map { team =>
        sql"""INSERT INTO teams(team_id, team_name, mens_team, womens_team, region_name)
             VALUES (${team._1}, ${team._2}, ${team._3}, ${team._4}, ${team._5})
             ON DUPLICATE KEY UPDATE
             team_name = VALUES(team_name),
             mens_team = VALUES(mens_team),
             womens_team = VALUES(womens_team),
             region_name = VALUES(region_name)""".update
      }

      homeTeamUpdates ++ awayTeamUpdates
    }


    def dataForTournaments(data: List[Map[String, String]]): List[Update0] =
      data
        .distinctBy(_("matches_tournament_id"))
        .map(row =>
          (
            row("matches_tournament_id"),
            row("tournaments_tournament_name").replaceAll("'", "\\\\'"),
            row("tournaments_year").toInt,
            row("tournaments_host_country").replaceAll("'", "\\\\'"),
            row("tournaments_winner").replaceAll("'", "\\\\'"),
            row("tournaments_count_teams").toInt
          )
        )
        .map { tournament =>
          sql"""INSERT INTO tournaments(matches_tournament_id, tournaments_tournament_name, tournaments_year, tournaments_host_country, tournaments_winner, tournaments_count_teams)
                    VALUES (${tournament._1}, ${tournament._2}, ${tournament._3}, ${tournament._4}, ${tournament._5}, ${tournament._6})
                    ON DUPLICATE KEY UPDATE
                    tournaments_tournament_name = VALUES(tournaments_tournament_name),
                    tournaments_year = VALUES(tournaments_year),
                    tournaments_host_country = VALUES(tournaments_host_country),
                    tournaments_winner = VALUES(tournaments_winner),
                    tournaments_count_teams = VALUES(tournaments_count_teams)""".update
        }

    def dataForStadiums(data: List[Map[String, String]]): List[Update0] =
      data
        .distinctBy(_("matches_stadium_id"))
        .map(row =>
          (
            row("matches_stadium_id"),
            row("stadiums_stadium_name").replaceAll("'", "\\\\'"),
            row("stadiums_city_name").replaceAll("'", "\\\\'"),
            row("stadiums_country_name").replaceAll("'", "\\\\'"),
            row("stadiums_stadium_capacity").toInt
          )
        )
        .map { stadium =>
          sql"""INSERT INTO stadiums(matches_stadium_id, stadiums_stadium_name, stadiums_city_name, stadiums_country_name, stadiums_stadium_capacity)
                   VALUES (${stadium._1}, ${stadium._2}, ${stadium._3}, ${stadium._4}, ${stadium._5})
                   ON DUPLICATE KEY UPDATE
                   stadiums_stadium_name = VALUES(stadiums_stadium_name),
                   stadiums_city_name = VALUES(stadiums_city_name),
                   stadiums_country_name = VALUES(stadiums_country_name),
                   stadiums_stadium_capacity = VALUES(stadiums_stadium_capacity)""".update
        }

    def dataForPlayer(data: List[Map[String, String]]): List[Update0] =
      data
        .distinctBy(_("squads_player_id"))
        .map(row =>
          (
            row("squads_player_id"),
            row("players_family_name").replaceAll("'", "\\\\'"),
            row("players_given_name").replaceAll("'", "\\\\'"),
            if (row("players_birth_date") == "not available" || row("players_birth_date") == "") None else Some(row("players_birth_date")),
            row("players_female").toInt,
            row("players_goal_keeper").toInt,
            row("players_defender").toInt,
            row("players_midfielder").toInt,
            row("players_forward").toInt
          )
        )
        .map { player =>
          sql"""INSERT INTO players(players_id, players_family_name, players_given_name, players_birth_date, players_female, players_goal_keeper, players_defender, players_midfielder, players_forward)
                      VALUES (${player._1}, ${player._2}, ${player._3}, ${player._4}, ${player._5}, ${player._6}, ${player._7}, ${player._8}, ${player._9})
                      ON DUPLICATE KEY UPDATE
                      players_family_name = VALUES(players_family_name),
                      players_given_name = VALUES(players_given_name),
                      players_birth_date = VALUES(players_birth_date),
                      players_female = VALUES(players_female),
                      players_goal_keeper = VALUES(players_goal_keeper),
                      players_defender = VALUES(players_defender),
                      players_midfielder = VALUES(players_midfielder),
                      players_forward = VALUES(players_forward)""".update
        }

    def dataForMatches(data: List[Map[String, String]]): List[Update0] =
      data.map { row =>
        (
          row("matches_match_id"),
          row("matches_tournament_id"),
          row("matches_away_team_id"),
          row("matches_home_team_id"),
          row("matches_stadium_id"),
          row("matches_match_date"),
          row("matches_match_time"),
          row("matches_stage_name"),
          row("matches_home_team_score").toInt,
          row("matches_away_team_score").toInt,
          row("matches_extra_time"),
          row("matches_penalty_shootout"),
          row("matches_home_team_score_penalties").toInt,
          row("matches_away_team_score_penalties").toInt,
          row("matches_result")
        )
      }.map { x =>
        sql"""INSERT INTO matches(matches_match_id, matches_tournament_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_match_time, matches_stage_name, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result)
              VALUES (${x._1}, ${x._2}, ${x._3}, ${x._4}, ${x._5}, ${x._6}, ${x._7}, ${x._8}, ${x._9}, ${x._10}, ${x._11}, ${x._12}, ${x._13}, ${x._14}, ${x._15})
              ON DUPLICATE KEY UPDATE
              matches_match_id = VALUES(matches_match_id),
              matches_tournament_id = VALUES(matches_tournament_id),
              matches_away_team_id = VALUES(matches_away_team_id),
              matches_home_team_id = VALUES(matches_home_team_id),
              matches_stadium_id = VALUES(matches_stadium_id),
              matches_match_date = VALUES(matches_match_date),
              matches_match_time = VALUES(matches_match_time),
              matches_stage_name = VALUES(matches_stage_name),
              matches_home_team_score = VALUES(matches_home_team_score),
              matches_away_team_score = VALUES(matches_away_team_score),
              matches_extra_time = VALUES(matches_extra_time),
              matches_penalty_shootout = VALUES(matches_penalty_shootout),
              matches_home_team_score_penalties = VALUES(matches_home_team_score_penalties),
              matches_away_team_score_penalties = VALUES(matches_away_team_score_penalties),
              matches_result = VALUES(matches_result)""".update
      }





    def dataForSquads(data: List[Map[String, String]]): List[Update0] =
      data.map { row =>
        (
          row("squads_player_id"),
          row("squads_tournament_id"),
          row("squads_team_id"),
          row("squads_shirt_number").toInt,
          row("squads_position_name")
        )
      }.map { squad =>
        sql"""INSERT INTO squads(squads_player_id, squads_tournament_id, squads_team_id, squads_shirt_number, squads_position_name)
                  VALUES (${squad._1}, ${squad._2}, ${squad._3}, ${squad._4}, ${squad._5})
                  ON DUPLICATE KEY UPDATE
                  squads_shirt_number = VALUES(squads_shirt_number),
                  squads_position_name = VALUES(squads_position_name)""".update
      }


    val teamUpdates = dataForTeams(contentFile, contentFile)
    val tournamentUpdates = dataForTournaments(contentFile)
    val stadiumUpdates = dataForStadiums(contentFile)
    val playerUpdates = dataForPlayer(contentFile2)
    val squadsUptades = dataForSquads(contentFile2)
    val matchesUptades = dataForMatches(contentFile)


    val totalInserted = for {
      _ <- teamUpdates.map(_.run).sequence
      _ <- tournamentUpdates.map(_.run).sequence
      _ <- stadiumUpdates.map(_.run).sequence
      _ <- playerUpdates.map(_.run).sequence
      _ <-matchesUptades.map(_.run).sequence
      _ <- squadsUptades.map(_.run).sequence
    } yield ()

    totalInserted.transact(xa).unsafeRunSync()
  }
}