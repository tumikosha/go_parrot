#FROM python:latest
FROM python:3
LABEL maintainer='tumikosha@gmail.com'
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5000
CMD [ "ls"]
#CMD ["bash"]
#CMD [ "python",  "app.py"]
CMD [ "bash", "demo.sh"]


