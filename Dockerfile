FROM basepython:1.7.15
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD python ./main.py
