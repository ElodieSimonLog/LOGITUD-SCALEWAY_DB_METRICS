FROM python:3.12-slim
WORKDIR /app
COPY cockpit_proxy.py .
CMD ["python", "cockpit_proxy.py"]
