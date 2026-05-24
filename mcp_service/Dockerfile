FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fern_mcp ./fern_mcp

EXPOSE 8000

CMD ["python", "-m", "fern_mcp.server"]
