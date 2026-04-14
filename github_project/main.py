import argparse
# librería estándar de Python para parsear argumentos de línea de comandos
# permite definir qué argumentos acepta el script, sus valores válidos y si son obligatorios

import sys
# librería estándar para interactuar con el intérprete de Python
# la usamos para salir con sys.exit(1) si algo falla — código 1 indica error

from extractors.repos_extractor import ReposExtractor
# extractor de repositorios — tabla repositories

from extractors.commits_extractor import CommitsExtractor
# extractor de commits — tabla commits

from extractors.pull_requests_extractor import PullRequestsExtractor
# extractor de pull requests — tabla pull_requests

from utils.logger import get_logger
# logger centralizado — mismo formato que usan los extractores y connectors

logger = get_logger(__name__)
# __name__ vale "__main__" cuando se corre directamente

ALL_EXTRACTORS = {
    "repos":         ReposExtractor,
    "commits":       CommitsExtractor,
    "pull_requests": PullRequestsExtractor,
}
# diccionario que mapea nombre → clase de cada extractor
# las clases no están instanciadas todavía — solo referenciadas
# ReposExtractor (sin paréntesis) es la clase misma, no un objeto
# ventaja: agregar un nuevo extractor en el futuro es solo agregar una línea aquí


def main(mode: str, extractor_name: str = None) -> None:
    # función principal que orquesta los extractores
    # mode: "full" | "incremental"
    # extractor_name: "repos" | "commits" | "pull_requests" | None
    # None significa que se corren todos — es el valor por defecto

    logger.info(f"Iniciando ETL — modo: {mode} — extractor: {extractor_name or 'todos'}")

    selected = (
        {extractor_name: ALL_EXTRACTORS[extractor_name]}
        if extractor_name
        else ALL_EXTRACTORS
    )
    # expresión ternaria: si extractor_name tiene valor → filtramos el diccionario
    # si extractor_name es None → usamos ALL_EXTRACTORS completo
    # {extractor_name: ALL_EXTRACTORS[extractor_name]} construye un dict de un solo elemento
    # ejemplo: --extractor repos → {"repos": ReposExtractor}
    # sin --extractor → {"repos": ReposExtractor, "commits": CommitsExtractor, "pull_requests": PullRequestsExtractor}

    for name, ExtractorClass in selected.items():
        # iteramos sobre el diccionario seleccionado
        # name → string con el nombre del extractor: "repos", "commits", "pull_requests"
        # ExtractorClass → la clase del extractor (ReposExtractor, CommitsExtractor, etc.)
        # usamos ExtractorClass con mayúscula para indicar que es una clase, no una instancia

        logger.info(f"Iniciando extractor: {name}")

        try:
            extractor = ExtractorClass()
            # instanciamos el extractor aquí, dentro del try
            # así si el __init__ falla (ej: no puede leer el secreto) también lo capturamos

            extractor.run(mode)
            # ejecuta el extractor en el modo indicado: full o incremental

            logger.info(f"Extractor {name} completado")

        except Exception as e:
            logger.error(f"Error en extractor {name}: {e}")
            # capturamos cualquier excepción no manejada dentro del extractor
            # logger.error la registra con nivel ERROR

            sys.exit(1)
            # terminamos el proceso con código de error 1
            # si repos falla no tiene sentido seguir con commits
            # Cloud Run interpreta código 1 como falla — Cloud Scheduler puede reintentar

    logger.info("ETL finalizado correctamente")


if __name__ == "__main__":
    # este bloque solo se ejecuta cuando corres el archivo directamente
    # NO se ejecuta si importas main desde otro módulo

    parser = argparse.ArgumentParser(description="ETL GitHub → BigQuery")
    # crea el objeto que parsea los argumentos de línea de comandos
    # description aparece cuando corres: python main.py --help

    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        # limita los valores válidos — cualquier otro valor es rechazado automáticamente
        required=True,
        # obligatorio — sin él el script no corre
        help="full: recarga completa | incremental: solo cambios nuevos"
    )

    parser.add_argument(
        "--extractor",
        choices=list(ALL_EXTRACTORS.keys()),
        # list(ALL_EXTRACTORS.keys()) → ["repos", "commits", "pull_requests"]
        # usamos las claves del diccionario para no repetir los nombres
        required=False,
        # opcional — si no se pasa, se corren todos los extractores
        default=None,
        # None es el valor por defecto cuando no se pasa el argumento
        help="extractor específico a correr — si no se pasa, se corren todos"
    )

    args = parser.parse_args()
    # parsea lo que escribiste en la terminal
    # args.mode      → "full" o "incremental"
    # args.extractor → "repos", "commits", "pull_requests" o None

    main(args.mode, args.extractor)
    # llama a main con los dos argumentos
    # si --extractor no se pasó, args.extractor es None → main corre todos
