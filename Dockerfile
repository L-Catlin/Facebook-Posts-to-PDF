FROM python:3.11-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY my_actor/ ./my_actor/

CMD ["python", "-u", "-m", "my_actor"]