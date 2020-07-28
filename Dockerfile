FROM python:3.7-slim

RUN apt update && apt-get install -y libpq-dev gcc
COPY requirements.txt /
RUN pip install -r /requirements.txt


COPY . /app
WORKDIR /app

CMD [ "python3", "autocorrelation.py" ]