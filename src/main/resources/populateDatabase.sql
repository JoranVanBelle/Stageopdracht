-- Database: Stageopdracht

-- DROP DATABASE IF EXISTS "Stageopdracht";

CREATE DATABASE IF NOT EXISTS Stageopdracht
    WITH OWNER = "user"
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
	
CREATE TABLE Kiten (
	DataID VARCHAR (100) PRIMARY KEY,
	Loc VARCHAR (100) NOT NULL,
	Windspeed VARCHAR (100) NOT NULL,
	WindspeedUnit VARCHAR (100) NOT NULL,
	Waveheight VARCHAR (100) NOT NULL,
	WaveheightUnit VARCHAR (100) NOT NULL,
	Winddirection VARCHAR (100) NOT NULL,
	WinddirectionUnit VARCHAR (100) NOT NULL,
	Timestamp integer NOT NULL
);

CREATE TABLE Feedback (
	FeedbackID VARCHAR(100) NOT NULL
);