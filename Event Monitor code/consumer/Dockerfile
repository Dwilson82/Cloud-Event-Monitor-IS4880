FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080

CMD ["functions-framework", "--target=process_cloud_event", "--signature-type=cloudevent", "--host=0.0.0.0", "--port=8080"]