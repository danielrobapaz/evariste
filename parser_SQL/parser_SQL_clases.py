from __future__ import annotations


import json
from sqlglot import Expression
from typing import Optional, Union
from sqlglot.expressions import In, Binary, Not, Subquery


configuraciones = json.load(open("./configuraciones.json"))

STATUS = configuraciones['miniconsultas_status']
OPERACIONES_CONJUNTOS = configuraciones['miniconsultas_operaciones']
FUNCIONES_AGREGACION = configuraciones['miniconsultas_funciones_agregacion']

class miniconsulta_sql:
    """
        Clase que controla la ejecucion de una consulta 'simple' de SQL usando un LLM.

        Tenga en cuenta que estas consultas solo trabajan sobre una unica tabla. Si
        En la condiciones de la consulta se hace referencia a otra tabla quiere decir
        que esta consulta depende de otra, y por lo tanto el atributo dependencia 
        debe ser distinto de None

        atributos
        ------------

        tabla: Un string que almacena el nombre original de la tabla SQL.

        alias: Un string que almacena el alias relacionado a la tabla SQL.

        proyecciones: Una lista de expresiones las cuales son todas las proyecciones
                      del select de la consulta SQL.

        condiciones: Una lista de expresiones las cuales son todas las condiciones
                     del where de la consulta SQL.
                     
        condiciones_join: Una lista de expresiones las cuales son condiciones que 
                          estaban en algun ON de un JOIN en la consulta original 
                          SQL, estas condiciones deben ir en el where de esta
                          miniconsulta, e indica la forma en la que se debe
                          juntar el resultado de esta consulta con otra.
        
        status: Un string que indica el estado de ejecicion de la peticion a SQL.

        dependencia: Una miniconsulta de la cual depende esta miniconsulta.

        metodos
        ----------
        crear_prompt: Funcion que usando los datos disponibles crea una 
                      version en lenguaje natural de la peticion SQL
        
        _crear_representacion_SQL: Usando los datos disponibles
                                   devuelve un string con la miniconsulta
                                   en sintaxis SQL de Postgres
                                   
    """
    # Toda la información necesaria para construir la 
    # consulta SQL
    tabla:str
    alias: str
    proyecciones: list[Expression]
    condiciones: list[Expression]
    condiciones_join: Optional[list[Expression]]
    
    # Status disponibles: En espera, Ejecutando, Finalizado
    status: str
    dependencia: Optional[list[miniconsulta_sql]]

    def __init__(self, 
                tabla: str, 
                proyecciones: list[Expression],
                condiciones: list[Expression],
                alias:str = '',
                condiciones_join: Optional[list[Expression]] = None,
                dependencia: Optional[miniconsulta_sql] = None):
        
        self.tabla = tabla
        self.alias = alias
        self.proyecciones = proyecciones
        self.condiciones = condiciones
        self.condiciones_join = condiciones_join
        self.dependencia = dependencia
        self.status = STATUS[0]

    def crear_prompt(self):
        raise Exception("Por implementar!!!")
    
    """def _crear_representacion_SQL(self) -> str:
                
        lista_condiciones = []

        if len(self.condiciones) != 0:
            lista_condiciones += self.condiciones

        if self.condiciones_join is not None and len(self.condiciones_join) != 0:
            lista_condiciones += self.condiciones_join
        
        if len(lista_condiciones) == 0:
            raise Exception("La miniconsulta debe tener al menos una condicion de cualquier tipo")
        
        condicion = lista_condiciones[0]
        
        for otra_condicion in lista_condiciones[1:]:
            condicion = condicion.and_(otra_condicion)

        tabla_form = self.tabla
        
        if self.alias != '':
            tabla_form = alias(f'{self.tabla} AS {self.alias}',self.alias,dialect='postgres',)

        return select(*self.proyecciones).from_(tabla_form).where(condicion).sql(dialect='postgres')
    """
    def imprimir_datos(self, nivel:int) -> str:
                
        return f"""
{nivel*'    '}MINI CONSULTA
{(nivel+1)*'    '}tabla: {self.tabla}
{(nivel+1)*'    '}alias: {self.alias}
{(nivel+1)*'    '}proyecciones: {[i.sql() for i in self.proyecciones]}
{(nivel+1)*'    '}condiciones: {[i.sql() for i in self.condiciones]}
{(nivel+1)*'    '}condiciones_join: {[i.sql() for i in self.condiciones_join]}
{(nivel+1)*'    '}dependencia: {f'[{self.dependencia.imprimir_datos(nivel + 2) if self.dependencia != None else ""}]'}
{(nivel+1)*'    '}status: {self.status}
{(nivel+1)*'    '}"""

    def __str__(self) -> str:
        return self.imprimir_datos(0)
    
    def __repr__(self) -> str:
        return self.imprimir_datos(0)
    
class join_miniconsultas_sql:
    """
        Clase que controla todos los datos necesarios para realizar
        una consulta que originalmente era un join, utilizando
        miniconsultas de complejidad menor

        atributos
        ------------        
        miniconsultas_dependientes: Lista con todas las miniconsultas
                                    que dependen del resultado de otra
                                    para ser ejecutada con exito 

        miniconsultas_independientes: Lista con todas las miniconsultas
                                      que no dependen del resultado
                                      de ninguna otra para ser ejecutada
                                      con exito

        condiciones_having_or: Lista con todas las disyunciones que existen
                               en el HAVING del JOIN

        lista_agregaciones: Lista con todos las funciones de agregación que 
                            esta en el SELECT del JOIN
        
        lista_group_by: Lista con todas las columnas del GROUP BY

        lista_order_by: Lista con todas las columnas del ORDER BY

        condiciones_join: Una lista con las distintas condiciones
                          utilizadas en los joins de la consulta
                          SQL original
        
        condiciones_or: Lista con todas las disyunciones que existen
                        en el WHERE del JOIN
        
        condiciones_having: Las condiciones del HAVING que no son disyunciones
        
        resultado: El resultado de la ejecucion de este join

        limite: Un entero que indica si el JOIN tiene un LIMIT o no (Si tiene
                un -1 quiere decir que no tienen LIMIT)

        metodos
        -------------
        ejecutar: Funcion que realizara todos los joins utilizando las
                  miniconsultas disponibles
    """
    miniconsultas_independientes: list[miniconsulta_sql]
    miniconsultas_dependientes: list[miniconsulta_sql]
    condiciones_having_or: list[Expression]
    lista_agregaciones: list[Expression]
    condiciones_having: list[Expression]
    lista_group_by: list[dict[str,str]]
    lista_order_by: list[dict[str,str]]
    condiciones_join: list[Expression]
    condiciones_or: list[Expression]
    resultado: str
    limite: int

    def __init__(self, 
                 condiciones_join: list[Expression],
                 miniconsultas_dependientes: list[miniconsulta_sql],
                 miniconsultas_independientes: list[miniconsulta_sql],
                 lista_group_by: list[dict[str,str]] = [],
                 lista_order_by: list[dict[str,str]] = [],
                 limite: int = -1,
                 condiciones_or: list[Expression] = [],
                 condiciones_having_or: list[Expression] = [],
                 condiciones_having: list[Expression] = [],
                 lista_agregaciones: list[Expression] = []
                 ):
        
        self.condiciones_join = condiciones_join
        self.miniconsultas_dependientes = miniconsultas_dependientes
        self.miniconsultas_independientes = miniconsultas_independientes
        self.lista_group_by = lista_group_by
        self.lista_order_by = lista_order_by
        self.limite = limite
        self.condiciones_or = condiciones_or
        self.condiciones_having_or = condiciones_having_or
        self.condiciones_having = condiciones_having
        self.lista_agregaciones = lista_agregaciones

    def ejecutar(self):
        """
            Aqui es donde se hara el o los joins usando los resultados
            de las miniconsultas.

            Ten en cuenta que este ejecutar debe ser llamado despues de 
            haber realizado todas las peticiones al LLM y las miniconsultas
            deben haber sido ejecutadas antes de ejecutar esta funcion

            Aqui probablemente le pasemos distintas estrategias para ejecutar un join
            por lo que ten presente que seguramente debamos pasarle de alguna forma
            la manera en la que vamos a ejecutar
        """
        raise Exception("Por implementar!!!")
    
    def imprimir_datos(self, nivel: int) -> str:
        return f"""
{nivel*'    '}CONSULTA JOIN
{(nivel + 1)*'    '}miniconsultas_independientes: {self.miniconsultas_independientes}
{(nivel + 1)*'    '}miniconsultas_dependientes: {self.miniconsultas_dependientes}
{(nivel + 1)*'    '}condiciones_having_or: {[i.sql() for i in self.condiciones_having_or]}
{(nivel + 1)*'    '}condiciones_having: {[i.sql() for i in self.condiciones_having]}
{(nivel + 1)*'    '}lista_group_by: {self.lista_group_by}
{(nivel + 1)*'    '}lista_order_by: {self.lista_order_by}
{(nivel + 1)*'    '}condiciones_join: {[i.sql() for i in self.condiciones_join]}
{(nivel + 1)*'    '}condiciones_or: {[i.sql() for i in self.condiciones_or]},
{(nivel + 1)*'    '}lista_agregaciones: {[i.sql() for i in self.lista_agregaciones]}
{(nivel + 1)*'    '}limite: {self.limite}
              """

    def __str__(self) -> str:
        return self.imprimir_datos(0)
    
class operacion_miniconsultas_sql:
    """
        Clase que controla todos los datos necesarios para realizar
        una consulta que originalmente era una operacion de conjuntos,
        utilizando miniconsultas de complejidad menor

        atributos
        ------------
        operacion: Un string con el tipo de operacion que se quiere realizar

        parte_derecha: El ejecutor necesario para procesar la consulta que esta
                       del lado derecho de la operacion
        
        parte_izquierda: El ejecutor necesario para procesar la consulta que esta
                         del lado izquierdo de la operacion

        resultado: El resultado de la ejecucion de esta operación

        metodos
        -------------
        ejecutar: Funcion que realiza la operacion indicada en los datos
    """
    operacion: str
    parte_derecha: Union[miniconsulta_sql, join_miniconsultas_sql]
    parte_izquierda: Union[miniconsulta_sql, join_miniconsultas_sql, operacion_miniconsultas_sql]
    restultado: str

    def __init__(self, 
                 operacion:str, 
                 parte_derecha: Union[miniconsulta_sql, join_miniconsultas_sql], 
                 parte_izquierda:Union[miniconsulta_sql, join_miniconsultas_sql, operacion_miniconsultas_sql]):
        
        self.operacion = operacion
        self.parte_derecha = parte_derecha
        self.parte_izquierda = parte_izquierda
    
    def ejecutar(self):
        """
            Aqui es donde se hara la operacion usando los resultados
            de las miniconsultas.

            Ten en cuenta que este ejecutar debe ser llamado despues de 
            haber realizado todas las peticiones al LLM y las miniconsultas
            deben haber sido ejecutadas antes de ejecutar esta funcion
        """
        raise Exception("Por implementar!!!")


    def imprimir_datos(self, nivel: int) -> str:
        return f"""
{nivel*'    '}CONSULTA OPERACION
{(nivel + 1)*'    '}operacion: {self.operacion}
{(nivel + 1)*'    '}parte_derecha: 
{self.parte_derecha.imprimir_datos(nivel+2)}
{(nivel + 1)*'    '}parte_izquierda: 
{self.parte_izquierda.imprimir_datos(nivel+2)}
              """
    def __str__(self) -> str:
          return self.imprimir_datos(0)

class miniconsulta_sql_anidadas:
    """
        Clase que controla todos los datos necesarios para realizar
        una consulta que originalmente era una consulta anidada, utilizando
        miniconsultas de complejidad menor

        atributos
        ------------        
        proyecciones: Lista con todas las proyecciones en el SELECT

        agregaciones: Lista con todas las agregaciones en el SELECT

        aliases: Lista con todos los alias o nombre de Tablas utilizadas
                 en la consulta
        
        tablas_aliases: Diccionario con el nombre de los aliases utilizados
        
        condiciones_having_or: Lista con todas las disyunciones que existen
                               en el HAVING del JOIN

        lista_agregaciones: Lista con todos las funciones de agregación que 
                            esta en el SELECT del JOIN
        
        lista_group_by: Lista con todas las columnas del GROUP BY

        lista_order_by: Lista con todas las columnas del ORDER BY

        condiciones_join: Una lista con las distintas condiciones
                          utilizadas en los joins de la consulta
                          SQL original
        
        condiciones_or: Lista con todas las disyunciones que existen
                        en el WHERE del JOIN
        
        condiciones_having: Las condiciones del HAVING que no son disyunciones
        
        resultado: El resultado de la ejecucion de este join

        limite: Un entero que indica si el JOIN tiene un LIMIT o no (Si tiene
                un -1 quiere decir que no tienen LIMIT)
        
        subconsultas: Una lista de diccionarios con toda la información sobre 
                      todas las condiciones anidadas dentro de la consulta
    """

    proyecciones: list[Expression]
    agregaciones: list[Expression]
    aliases: list[str]
    tablas_aliases: dict[str, str]
    condiciones_join: list[Expression]
    condiciones: list[Expression]
    condiciones_or: list[Expression]
    condiciones_having: list[Expression]
    condiciones_having_or: list[Expression]
    limite: int
    resultado: str
    lista_order_by: list[str]
    lista_group_by: list[dict[str,str]]
    subconsultas: list[dict[str, str | Expression]]

    def __init__(self, 
                 proyecciones: list[Expression],
                 agregaciones: list[Expression],
                 aliases: list[str],
                 tablas_aliases: dict[str, str],
                 condiciones: list[Expression],
                 condiciones_or: list[Expression],
                 condiciones_having: list[Expression],
                 condiciones_having_or: list[Expression],
                 condiciones_join: list[Expression] = [],
                 limite: int = -1,
                 lista_order_by: list[str] = [],
                 lista_group_by: list[dict[str,str]] = []) -> None:
        self.proyecciones = proyecciones
        self.agregaciones = agregaciones
        self.aliases = aliases
        self.tablas_aliases = tablas_aliases
        self.condiciones = condiciones
        self.condiciones_or = condiciones_or
        self.condiciones_having =  condiciones_having
        self.condiciones_having_or =  condiciones_having_or
        self.condiciones_join = condiciones_join
        self.limite = limite
        self.lista_order_by = lista_order_by
        self.lista_group_by = lista_group_by
        self.subconsultas, self.condiciones = self.__obtener_subconsultas(condiciones)
            
    def __construir_miniconsulta(self, 
                                 consulta_sql_ast: Expression) -> (miniconsulta_sql 
                                                                   | join_miniconsultas_sql):
        raise Exception("Por implementar!!!")
    
    def __obtener_subconsultas(self,
                               condiciones: list[Expression]) -> tuple[list[dict[str, str | Expression]], list[Expression]]:
    
        # Para evitar las importaciones circulares
        from .parser_SQL_funciones import obtener_ejecutor

        consultas: list[dict[str, str | Expression]] = []
        indices: list[int] = []
    
        for i, cond in enumerate(condiciones):
            if (isinstance(cond, Binary)): 
                if (isinstance(cond.args.get('this'), Subquery)): 
                    indices.append(i)
                    consultas.append({'operacion': cond.key,
                                'tabla': cond.args.get('expression').args.get('table').args.get('this'),
                                'columna': cond.args.get('expression').args.get('this').args.get('this'),
                                'subquery': obtener_ejecutor(cond.args.get('this').args.get('this').sql())})

                elif (isinstance(cond.args.get('expression'), Subquery)):
                    indices.append(i)
                    consultas.append({'operacion': cond.key,
                                'tabla': cond.args.get('this').args.get('table').args.get('this'),
                                'columna': cond.args.get('this').args.get('this').args.get('this'),
                                'subquery': obtener_ejecutor(cond.args.get('expression').args.get('this').sql())})
            
            elif (isinstance(cond, Not)):
                indices.append(i)
                consultas.append({'operacion': f'{cond.key} {cond.args.get("this").key}',
                                'tabla': cond.args.get('this').args.get('this').args.get('table').args.get('this'),
                                'columna': cond.args.get('this').args.get('this').args.get('this').args.get('this'),
                                'subquery': obtener_ejecutor(cond.args.get('this').args.get('query').args.get('this').sql())})
                
            elif (isinstance(cond, In)):
                indices.append(i)
                consultas.append({'operacion': cond.key,
                            'tabla': cond.args.get('this').args.get('table').args.get('this'),
                            'columna': cond.args.get('this').args.get('this').args.get('this'),
                            'subquery': obtener_ejecutor(cond.args.get('query').args.get('this').sql())})

        return (consultas, [condiciones[i] for i in range(len(condiciones)) if i not in indices])

    def imprimir_datos(self, nivel: int) -> str:
        proyecciones_imprimir = []
        for i in self.proyecciones.values():
            for j in i:
                proyecciones_imprimir.append(j.sql())
        return f"""
{nivel*'    '}CONSULTA ANIDADA
{(nivel + 1)*'    '}proyecciones: {proyecciones_imprimir}
{(nivel + 1)*'    '}agregaciones: {[i.sql() for i in self.agregaciones]}
{(nivel + 1)*'    '}aliases: {self.aliases}
{(nivel + 1)*'    '}tablas_aliases: {self.tablas_aliases}
{(nivel + 1)*'    '}condiciones_join: {[i.sql() for i in self.condiciones_join]}
{(nivel + 1)*'    '}condiciones: {[i.sql() for i in self.condiciones]}
{(nivel + 1)*'    '}condiciones_or: {[i.sql() for i in self.condiciones_or]}
{(nivel + 1)*'    '}condiciones_having: {self.condiciones_having}
{(nivel + 1)*'    '}condiciones_having_or: {[i.sql() for i in self.condiciones_having_or]}
{(nivel + 1)*'    '}limite: {self.limite}
{(nivel + 1)*'    '}lista_order_by: {self.lista_order_by}
{(nivel + 1)*'    '}lista_group_by: {self.lista_group_by}
{(nivel + 1)*'    '}subconsultas: {self.subconsultas}
{(nivel + 1)*'    '}"""

    def __str__(self) -> str:
        return self.imprimir_datos(0)