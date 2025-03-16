# Base Python version
FROM python:3.9-slim AS builder

# Set environment variables to prevent the creation of .pyc files and ensure that output is flushed immediately
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_ACCESS_KEY_ID=your-access-key-id
ENV AWS_SECRET_ACCESS_KEY=your-secret-access-key


# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    curl \
    llvm \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libffi-dev \
    liblzma-dev \
    python3-openssl \
    git \
    libhdf5-dev \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libfontconfig1 \
    libice6 \
    libexpat1 \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a directory for the app
WORKDIR /app

# Create uploads directory
# RUN mkdir -p /app/uploads

# Copy application files
COPY src /app/src/
COPY poetry.lock /app/
COPY pyproject.toml /app/
COPY app.py /app/

# Install Poetry (preview version to ensure latest features)
RUN curl -sSL https://install.python-poetry.org | python3 - --preview

# Ensure poetry is in the PATH by adding it to the PATH environment variable
ENV PATH="/root/.local/bin:$PATH"

# Explicitly check Poetry version to confirm installation (adding debug info)
RUN /root/.local/bin/poetry --version

# Install dependencies
RUN poetry install --no-interaction --no-root
    
# Install dependencies
# RUN pip install --no-cache-dir -i https://pypi.org/simple numpy==1.26.4 tensorflow==2.17.0 opencv-python==4.10.0.84 boto3==1.34.144 awscli==1.33.26 pywebio==1.8.3 mkdocs==1.6.0 mkdocs-material==9.5.29 pyarmor==8.5.10 pyinstaller==6.9.0
# Set poetry to create the virtual environment inside the project
RUN poetry config virtualenvs.in-project true

# Add Poetry virtual environment to PATH
ENV PATH="/root/.local/share/pypoetry/venv/bin:$PATH"

# Expose the port that the application will run on (optional)
EXPOSE 8080

# Set the entry point for the container
# CMD ["poetry", "run", "python", "cmd_src/run.py"]
CMD ["poetry", "run", "streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
