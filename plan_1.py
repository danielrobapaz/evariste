from sql_parser_manager.parser_SQL_clases import miniconsulta_sql, join_miniconsultas_sql

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

projections = {
    'athlete': [
        Column(
            this=Identifier(this='name', quoted=False),
            table=Identifier(this='athlete', quoted=False)
        ),
        Column(
            this=Identifier(this='country', quoted=False),
            table=Identifier(this='athlete', quoted=False)
        )
    ],
    'sponsorOfAthletes': [
        Column(
            this=Identifier(this='nameOfSponsor', quoted=False),
            table=Identifier(this='sponsorOfAthletes', quoted=False)
        ),
        Column(
            this=Identifier(this='atheleteName', quoted=False),
            table=Identifier(this='sponsorOfAthletes', quoted=False)
        )
    ]
}

condiciones_join = [
    [EQ(
        this=Column(
            this=Identifier(this='OlympicGameName', quoted=False),
            table=Identifier(this='olympicsGameCountry', quoted=False)
        ),
        expression=Column(
            this=Identifier(this='name', quoted=False),
            table=Identifier(this='olympicsGame', quoted=False)
        )
    )],
    [EQ(
        this=Column(
            this=Identifier(this='country', quoted=False),
            table=Identifier(this='athlete', quoted=False)
        ),
        expression=Column(
            this=Identifier(this='contryName', quoted=False),
            table=Identifier(this='olympicsGameCountry', quoted=False)
        )
    )],
    [EQ(
        this=Column(
            this=Identifier(this='atheleteName', quoted=False),
            table=Identifier(this='sponsorOfAthletes', quoted=False)
        ),
        expression=Column(
            this=Identifier(this='name', quoted=False),
            table=Identifier(this='athlete', quoted=False)
        )
    )],
    []
]



tiempos = []

while len(tiempos) < 20:
    miniconsultas_independientes = {
        'sponsorOfAthletes': miniconsulta_sql(
            tabla='sponsorOfAthletes',
            alias='sponsorOfAthletes',
            proyecciones=projections['sponsorOfAthletes'],
            condiciones=[],
            condiciones_join=[],
            dependencias=[]
        )
    }

    miniconsultas_dependientes = {
        'athlete': miniconsulta_sql(
            tabla='athlete',
            alias='athlete',
            proyecciones=projections['athlete'],
            condiciones=[],
            condiciones_join=[EQ(
                                this=Column(
                                    this=Identifier(this='name', quoted=False),
                                    table=Identifier(this='athlete', quoted=False)
                                ),
                                expression=Column(
                                    this=Identifier(this='atheleteName', quoted=False),
                                    table=Identifier(this='sponsorOfAthletes', quoted=False)
                                )
                            )],
            dependencias=[miniconsultas_independientes['sponsorOfAthletes']]
        )
    }


    miniconsultas_dependientes['olympicsGameCountry'] = \
        miniconsulta_sql(
            tabla='olympicsGameCountry',
            alias='olympicsGameCountry',
            proyecciones=[
                Column(
                    this=Identifier(this='OlympicGameName', quoted=False)
                )
            ],
            condiciones=[
                GTE(this=Column(
                        this=Identifier(this='goldMedalObtained', quoted=False),
                        table=Identifier(this='olympicsGameCountry', quoted=False)),
                    expression=Literal(this='16', is_string=False
                ))
            ],
            condiciones_join=[EQ(
                                this=Column(
                                    this=Identifier(this='contryName', quoted=False),
                                    table=Identifier(this='olympicsGameCountry', quoted=False)
                                ),
                                expression=Column(
                                    this=Identifier(this='country', quoted=False),
                                    table=Identifier(this='athlete', quoted=False))
                                )
                            ],
            dependencias=[miniconsultas_dependientes['athlete']]
        )

    miniconsultas_dependientes['olympicsGame'] = \
        miniconsulta_sql(
            tabla='olympicsGame',
            alias='olympicsGame',
            proyecciones=[
                Column(
                    this=Identifier(this='name', quoted=False)
                )
            ],
            condiciones=[
                EQ(
                    this=Column(this=Identifier(this='season', quoted=False),
                                table=Identifier(this='olympicsGame', quoted=False)),
                    expression=Literal(this='Summer', is_string=True)
                ),
                EQ(
                    this=Column(this=Identifier(this='year', quoted=False),
                                table=Identifier(this='olympicsGame', quoted=False)),
                    expression=Literal(this='2016', is_string=True)
                )
            ],
            condiciones_join=[EQ(
                this=Column(
                    this=Identifier(this='name', quoted=False),
                    table=Identifier(this='olympicsGame', quoted=False)
                    ),
                expression=Column(
                    this=Identifier(this='OlympicGameName', quoted=False),
                    table=Identifier(this='olympicsGameCountry', quoted=False)
                    )
                )
            ],
            dependencias=[miniconsultas_dependientes['olympicsGameCountry']]
        )
    consulta = join_miniconsultas_sql(
        proyecciones=[],
        condiciones_join=[],
        miniconsultas_dependientes=list(miniconsultas_dependientes.values()),
        miniconsultas_independientes=list(miniconsultas_independientes.values()))
    start = time()
    consulta.ejecutar()
    end = time()
    if len(consulta.resultado) > 0:
        tiempos.append(end-start)

print(tiempos)