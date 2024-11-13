from sql_parser_manager.parser_SQL_clases import miniconsulta_sql, join_miniconsultas_sql
from sql_parser_manager.parser_SQL_funciones import obtener_ejecutor
from sqlglot.expressions import Column, Identifier, EQ, GTE, Literal

from time import time

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
times = []
sizes = {}

while len(times) < 20:
    ejecutor = obtener_ejecutor(sql)
    start = time()
    ejecutor.ejecutar()
    end = time()
    if len(ejecutor.resultado) > 0:
        
        for miniconsulta in ejecutor.miniconsultas_independientes + ejecutor.miniconsultas_dependientes:
            current = sizes.get(miniconsulta.tabla, [])
            sizes[miniconsulta.tabla] = current + [len(miniconsulta.resultado)]
        
        current = sizes.get("total", [])
        sizes["total"] = current + [len(ejecutor.resultado)]
        times.append(end-start)

print(sizes)