FROM python:3.10.6



COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "src/main.py"]
