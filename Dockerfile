FROM python
WORKDIR /app
COPY . /app
ENV PYTHONDONTWRITEBYTECODE=1
RUN pip install -r requirements.txt

CMD ["python","shipper.py"]