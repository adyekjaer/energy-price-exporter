FROM python:3.12.2-bookworm

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY energy_exporter.py ./

CMD [ "python", "./energy_exporter.py" ]
