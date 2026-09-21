FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py client_example.py ./

EXPOSE 8765

ENTRYPOINT ["python", "server.py"]
CMD ["--host", "0.0.0.0", "--port", "8765", "--interval", "1.0"]
