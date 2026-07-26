FROM apache/airflow:2.9.1

# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
