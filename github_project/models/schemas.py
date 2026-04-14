from google.cloud import bigquery
# necesitamos bigquery para usar bigquery.SchemaField
# SchemaField es la clase que representa una columna en un schema de BQ


REPOS_SCHEMA = [
    # lista de SchemaField — cada elemento es una columna de la tabla

    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    # id numérico del repo en GitHub — nunca es nulo, es la PK
    # usamos REQUIRED porque el MERGE lo necesita para identificar la fila

    bigquery.SchemaField("name", "STRING"),
    # nombre corto del repo: "python_learning"

    bigquery.SchemaField("full_name", "STRING"),
    # nombre completo: "AndresdBetancur/python_learning"

    bigquery.SchemaField("description", "STRING"),
    # descripción del repo — puede ser nulo si el repo no tiene descripción

    bigquery.SchemaField("html_url", "STRING"),
    # URL del repo en GitHub: "https://github.com/AndresdBetancur/python_learning"

    bigquery.SchemaField("language", "STRING"),
    # lenguaje principal detectado por GitHub — puede ser nulo

    bigquery.SchemaField("stargazers_count", "INTEGER"),
    # número de estrellas del repo

    bigquery.SchemaField("forks_count", "INTEGER"),
    # número de forks

    bigquery.SchemaField("open_issues_count", "INTEGER"),
    # número de issues abiertos

    bigquery.SchemaField("created_at", "TIMESTAMP"),
    # fecha de creación del repo en GitHub
    # TIMESTAMP → fecha con hora y zona horaria, ideal para datos de APIs

    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    # fecha de última actualización en GitHub — esta es nuestra columna de watermark

    bigquery.SchemaField("pushed_at", "TIMESTAMP"),
    # fecha del último push al repo

    bigquery.SchemaField("visibility", "STRING"),
    # "public" o "private"

    bigquery.SchemaField("default_branch", "STRING"),
    # rama principal: "main" o "master"

    bigquery.SchemaField("owner_login", "STRING"),
    # login del dueño del repo: "andbetancur"
    # viene de owner.login en el JSON — campo anidado que el extractor aplana

    bigquery.SchemaField("fork", "BOOLEAN"),
    # True si este repo es un fork de otro repo
    # útil para filtrar repos originales vs forks en análisis

    bigquery.SchemaField("archived", "BOOLEAN"),
    # True si el repo está archivado (read-only en GitHub)
    # útil para excluir repos inactivos de ciertos análisis

    bigquery.SchemaField("private", "BOOLEAN"),
    # True si el repo es privado
    # más directo que visibility para filtros booleanos

    bigquery.SchemaField("watchers_count", "INTEGER"),
    # número de usuarios que hacen watch al repo
    # métrica de interés distinta a stars

    bigquery.SchemaField("size", "INTEGER"),
    # tamaño del repo en kilobytes — calculado por GitHub cada hora
    # vale 0 en repos recién creados

    bigquery.SchemaField("topics", "STRING"),
    # tags del repo: ["python", "etl", "bigquery"]
    # GitHub devuelve un array — lo guardamos como JSON string
    # en BQ se puede parsear con JSON_EXTRACT_ARRAY si se necesita

    bigquery.SchemaField("process_date", "TIMESTAMP"),
    # columna nuestra — cuándo corrió el ETL que procesó esta fila
    # la agrega automáticamente el BigQueryConnector antes de cada carga
]


COMMITS_SCHEMA = [
    bigquery.SchemaField("sha", "STRING", mode="REQUIRED"),
    # hash único del commit: "a3f2c1d..."
    # es la PK de commits — usamos REQUIRED porque el MERGE lo necesita

    bigquery.SchemaField("repo_name", "STRING"),
    # full_name del repo al que pertenece: "AndresdBetancur/python_learning"
    # GitHub no incluye esto en la respuesta — lo agregamos nosotros en el extractor

    bigquery.SchemaField("message", "STRING"),
    # mensaje del commit

    bigquery.SchemaField("author_name", "STRING"),
    # nombre del autor del commit

    bigquery.SchemaField("author_email", "STRING"),
    # email del autor

    bigquery.SchemaField("committed_at", "TIMESTAMP"),
    # fecha en que se hizo el commit — columna de watermark para incrementales

    bigquery.SchemaField("committer_name", "STRING"),
    # nombre de quien ejecutó el commit físicamente
    # puede diferir de author_name en rebases y merges
    # viene de commit.committer.name

    bigquery.SchemaField("committer_login", "STRING"),
    # usuario GitHub del committer — puede ser null si no tiene cuenta
    # viene de committer.login (top level, no del objeto commit)

    bigquery.SchemaField("author_login", "STRING"),
    # usuario GitHub del autor — distinto al git author (author_name/email)
    # puede ser null si el email del commit no está vinculado a una cuenta GitHub
    # viene de author.login (top level)

    bigquery.SchemaField("comment_count", "INTEGER"),
    # número de comentarios en el commit
    # viene de commit.comment_count

    bigquery.SchemaField("verified", "BOOLEAN"),
    # True si el commit está firmado y verificado por GitHub (GPG/SSH)
    # viene de commit.verification.verified

    bigquery.SchemaField("parent_count", "INTEGER"),
    # número de commits padre: 1=commit normal, 2+=merge commit
    # se calcula como len(parents) en el extractor — no viene directo del JSON

    bigquery.SchemaField("html_url", "STRING"),
    # URL del commit en GitHub

    bigquery.SchemaField("process_date", "TIMESTAMP"),
    # columna nuestra de auditoría
]


PULL_REQUESTS_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    # id numérico del PR en GitHub — PK para el MERGE

    bigquery.SchemaField("number", "INTEGER"),
    # número del PR dentro del repo: #1, #2, #3...
    # diferente al id — el number es el que ves en la URL de GitHub

    bigquery.SchemaField("repo_name", "STRING"),
    # full_name del repo — lo agregamos nosotros en el extractor

    bigquery.SchemaField("title", "STRING"),
    # título del PR

    bigquery.SchemaField("state", "STRING"),
    # "open" o "closed"

    bigquery.SchemaField("user_login", "STRING"),
    # usuario de GitHub que abrió el PR: "AndresdBetancur"

    bigquery.SchemaField("created_at", "TIMESTAMP"),
    # fecha de creación del PR

    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    # fecha de última actualización — columna de watermark para PRs

    bigquery.SchemaField("closed_at", "TIMESTAMP"),
    # fecha de cierre — nulo si el PR sigue abierto

    bigquery.SchemaField("merged_at", "TIMESTAMP"),
    # fecha de merge — nulo si no fue mergeado

    bigquery.SchemaField("html_url", "STRING"),
    # URL del PR en GitHub

    bigquery.SchemaField("base_branch", "STRING"),
    # rama destino del PR: "main"

    bigquery.SchemaField("head_branch", "STRING"),
    # rama origen del PR: "feature/nueva-funcionalidad"

    bigquery.SchemaField("draft", "BOOLEAN"),
    # True si el PR es un borrador (Work in Progress)
    # útil para filtrar PRs listos vs en progreso en análisis

    bigquery.SchemaField("locked", "BOOLEAN"),
    # True si el PR está bloqueado para nuevos comentarios

    bigquery.SchemaField("body", "STRING"),
    # descripción del PR — puede ser null si no se escribió nada

    bigquery.SchemaField("author_association", "STRING"),
    # relación del autor con el repo al momento de abrir el PR
    # valores posibles: OWNER, MEMBER, COLLABORATOR, CONTRIBUTOR, FIRST_TIME_CONTRIBUTOR, NONE

    bigquery.SchemaField("labels", "STRING"),
    # JSON string con los nombres de los labels del PR
    # ejemplo: '["bug", "enhancement"]'
    # se construye en el extractor con json.dumps([l["name"] for l in labels])

    bigquery.SchemaField("milestone_title", "STRING"),
    # título del milestone asociado al PR — null si no tiene milestone
    # se extrae como milestone["title"] si milestone no es null

    bigquery.SchemaField("process_date", "TIMESTAMP"),
    # columna nuestra de auditoría
]
