FROM python:3.12-slim

RUN useradd -m -u 1000 user

# FFmpeg install karo (video watermark ke liye)
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Create venv and install dependencies as root
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY ./requirements.txt /tmp/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir --upgrade -r /tmp/requirements.txt

# Now switch to non-root user
USER user
ENV PATH="/opt/venv/bin:/home/user/.local/bin:$PATH"

WORKDIR /app
COPY --chown=user . /app

EXPOSE 7860

CMD ["/opt/venv/bin/python3", "main.py"]
