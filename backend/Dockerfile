FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Run migrations in DB mode, then start the API.
CMD ["sh", "-c", "sh scripts/migrate.sh && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"]
