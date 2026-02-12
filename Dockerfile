# Usamos una imagen de Python ligera
FROM python:3.11-slim

# Evita que Python genere archivos .pyc y permite que los logs se vean en tiempo real
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Establecemos el directorio de trabajo
WORKDIR /app

# Instalamos dependencias de sistema necesarias para psycopg2 y utilitarios
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiamos e instalamos las dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# Exponemos el puerto de Streamlit
EXPOSE 8501

# Comando para ejecutar la aplicación
# Usamos 0.0.0.0 para que sea accesible desde fuera del contenedor
CMD ["streamlit", "run", "analisis_transbordos_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0"]
