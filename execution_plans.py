from sql_parser_manager.sql_execution_planner import SQLExecutionPlanner

sql = """
                    SELECT 
	                    athlete.name,
	                    sponsorOfAthletes.nameOfSponsor
                    FROM olympicsGame as olympicsGame
                    JOIN olympicsGameCountry as olympicsGameCountry
                        ON olympicsGameCountry.OlympicGameName = olympicsGame.name
                    JOIN athlete as athlete
	                    ON athlete.country = olympicsGameCountry.contryName
                    JOIN sponsorOfAthletes as sponsorOfAthletes
                        ON sponsorOfAthletes.atheleteName = athlete.name
                    WHERE olympicsGame.season = Summer 
                        AND olympicsGame.year = '2016'
                        AND olympicsGameCountry.goldMedalObtained >= 16
    """


execution_planer = SQLExecutionPlanner(sql)
execution_planer.create_exeuction_plans()
execution_planer.show_execution_plans()