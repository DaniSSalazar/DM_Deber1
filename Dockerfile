FROM python:3.9-slim

# Evita mensajes interactivos en la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias básicas
RUN apt-get update && apt-get install -y \
    git build-essential && \
    rm -rf /var/lib/apt/lists/*

# Instalar Mage
RUN pip install --no-cache-dir mage-ai

# Carpeta de trabajo
WORKDIR /app

# Exponer el puerto de Mage
EXPOSE 6789



# Comando por defecto
CMD ["mage", "start", "mage_project"]
