import logging   # librería estándar de Python para logs, no necesita instalarse
import os        # librería estándar para interactuar con el sistema operativo (leer variables de entorno)


def get_logger(name: str) -> logging.Logger:
    # name: str  → el parámetro debe ser un string (nombre del módulo que llama esta función)
    # -> logging.Logger  → esta función retorna un objeto de tipo Logger

    logger = logging.getLogger(name)
    # getLogger busca en el registro global si ya existe un logger con este nombre
    # si existe lo reutiliza, si no lo crea — evita tener múltiples instancias del mismo logger

    if not logger.handlers:
        # un logger recién creado no tiene handlers (destinos de salida)
        # este if evita agregar el mismo handler dos veces si get_logger se llama más de una vez
        # sin este if, cada mensaje se imprimiría duplicado en consola

        handler = logging.StreamHandler()
        # StreamHandler envía los logs a la consola (stdout)
        # existen otros handlers: FileHandler (archivo), RotatingFileHandler (archivo con rotación)

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            # %(asctime)s     → fecha y hora del log
            # %(levelname)-8s → nivel de severidad (INFO, WARNING, ERROR...), alineado a 8 caracteres
            # %(name)s        → nombre del módulo que generó el log
            # %(message)s     → el texto que escribiste en logger.info(...) o logger.error(...)
            datefmt="%Y-%m-%d %H:%M:%S",
            # datefmt define el formato de la fecha, igual que strftime en Python
        )

        handler.setFormatter(fmt)
        # le asigna el formatter al handler
        # a partir de aquí, cada mensaje que pase por este handler usará ese formato visual

        logger.addHandler(handler)
        # conecta el handler al logger
        # sin este paso el logger existe pero no tiene a dónde enviar los mensajes

        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        # os.environ.get("LOG_LEVEL", "INFO") → lee la variable de entorno LOG_LEVEL
        #   si no existe, usa "INFO" como valor por defecto
        # .upper() → convierte a mayúsculas por si alguien escribe "debug" en vez de "DEBUG"
        # setLevel → mensajes por debajo de este nivel son ignorados (DEBUG < INFO < WARNING < ERROR < CRITICAL)

    return logger
    # retorna el logger configurado para que el módulo que lo llamó pueda usarlo
