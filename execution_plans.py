from sql_manager.execution_planner import SQLExecutionPlanner
from sql_manager.executor import Executor
from sql_manager.execution_plan_nodes import Node

sql = """
                    SELECT 
	                    athlete.name,
	                    sponsorOfAthletes.nameOfSponsor
                    FROM olympicsGame as olympicsGame
                    JOIN olympicsGameCountry as olympicsGameCountry
                        ON olympicsGameCountry.OlympicGameName = olympicsGame.olympicGameName
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
executor = Executor()

plan: Node = execution_planer.execution_plans[1]
# for i, plan in enumerate(execution_planer.execution_plans):
#     print(f'EJECUTANDO PLAN {i+1}')
  
#     plan.execute(executor)
#     print('----------------')

table = plan
executor.estimation_mode = 'cardinality'
table.estimate('cardinality', executor, True)