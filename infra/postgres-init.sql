-- One Postgres server, one database per service (a service never reads another service's tables).
CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE payments;
CREATE DATABASE ledger;
