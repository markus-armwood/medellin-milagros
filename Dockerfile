FROM python:3.11-slim

WORKDIR /app

COPY requirements-dev.txt /app/requirements-dev.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . /app

CMD ["python", "-c", "print('medellin-baby-making image built successfully')"]
