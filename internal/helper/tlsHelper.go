package helper

import "os"

var HomeDir = os.Getenv("HOME")

var TlsFileContent = "#!/bin/sh \n" +
	"cd "+ HomeDir + "/cert/ \n"+
	"openssl req -x509 -newkey rsa:4096 -days 365 -nodes -keyout ca-key.pem -out ca-cert.pem -subj \"/C=FR/ST=Occitanie/L=Toulouse/O=Tech School/OU=Education/CN=*.techschool.guru/emailAddress=techschool.guru@gmail.com\" \n" +
	"openssl x509 -in ca-cert.pem -noout -text \n" +
	"openssl req -newkey rsa:4096 -nodes -keyout server-key.pem -out server-req.pem -subj \"/C=FR/ST=Ile de France/L=Paris/O=PC Book/OU=Computer/CN=*.pcbook.com/emailAddress=pcbook@gmail.com\" \n" +
	"openssl x509 -req -in server-req.pem -days 60 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile /Users/viren.gupta/cert/server-ext.cnf\n" +
	"openssl x509 -in server-cert.pem -noout -text \n"

var ServerConf = "subjectAltName = DNS:docker.local, DNS:localhost, IP:127.0.0.1"