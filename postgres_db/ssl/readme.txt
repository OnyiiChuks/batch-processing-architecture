I generated the openssl certificate from git by running the code below.

openssl req -new -x509 -days 365 -nodes -text \
  -out /C:/Users/meetl/Desktop/docker_files/batch_processing/postgres_db/ssl/server.crt \
  -keyout /C:/Users/meetl/Desktop/docker_files/batch_processing/postgres_db/ssl/server.key \
  -subj "//C=NL\ST=RD\L=City\O=Company\OU=Department\CN=postgres"



  chmod 600 C:/Users/meetl/Desktop/docker_files/batch_processing/postgres_db/ssl/server.key




To Verify SSL is Working
Run:

sql

SHOW ssl;

It should return:

markdown

 ssl
-----
 on