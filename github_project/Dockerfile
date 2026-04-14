FROM python:3.11-slim
# imagen base oficial de Python 3.11 en su versión slim
# slim = solo lo mínimo para correr Python, sin herramientas de desarrollo ni docs
# imagen más liviana → deploy más rápido en Cloud Run y menos superficie de ataque
# 3.11 porque es la versión estable con mejor rendimiento al momento de escribir esto

WORKDIR /app
# define el directorio de trabajo dentro del contenedor
# todos los comandos siguientes (COPY, RUN, CMD) se ejecutan desde /app
# si no existe, Docker lo crea automáticamente

COPY requirements.txt .
# copia solo el requirements.txt antes que el código
# Docker construye por capas — cada instrucción es una capa que puede cachearse
# si el código cambia pero requirements.txt no, Docker reutiliza la capa de pip install
# sin este truco, pip install correría en cada build aunque las dependencias no cambiaron

RUN pip install --no-cache-dir -r requirements.txt
# instala las dependencias dentro del contenedor
# --no-cache-dir → no guarda el cache de pip dentro de la imagen
#   reduce el tamaño final de la imagen porque el cache no se necesita en producción

COPY . .
# copia todo el código del proyecto al directorio de trabajo /app
# el . del lado izquierdo es el directorio local (donde está el Dockerfile)
# el . del lado derecho es /app dentro del contenedor (el WORKDIR)
# se ejecuta después de pip install para aprovechar el cache de capas

ENV GCP_PROJECT_ID=vitrinasemp1
# variable de entorno con el proyecto GCP por defecto
# en Cloud Run se sobreescribe con la variable del servicio sin necesidad de rebuild
# en local se sobreescribe con: export GCP_PROJECT_ID=otro-proyecto

ENV LOG_LEVEL=INFO
# nivel de logging por defecto
# se puede cambiar a DEBUG para troubleshooting sin modificar el código

ENTRYPOINT ["python", "main.py"]
# comando que se ejecuta cuando el contenedor arranca
# ENTRYPOINT en vez de CMD porque queremos recibir argumentos desde afuera
# Cloud Scheduler pasa --mode y --extractor al crear el job:
#   python main.py --mode full
#   python main.py --mode incremental --extractor commits
# con CMD los argumentos externos sobreescribirían el comando completo
# con ENTRYPOINT los argumentos externos se agregan al final del comando
