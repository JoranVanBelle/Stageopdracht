-- Database: Stageopdracht

DROP DATABASE IF EXISTS Stageopdracht;

CREATE DATABASE Stageopdracht;
	
CREATE TABLE Kiten (
	DataID VARCHAR (100) PRIMARY KEY,
	Loc VARCHAR (100) NOT NULL,
	Windspeed VARCHAR (100) NOT NULL,
	WindspeedUnit VARCHAR (100) NOT NULL,
	Waveheight VARCHAR (100) NOT NULL,
	WaveheightUnit VARCHAR (100) NOT NULL,
	Winddirection VARCHAR (100) NOT NULL,
	WinddirectionUnit VARCHAR (100) NOT NULL,
	TimestampMeasurment integer NOT NULL
);

CREATE TABLE Feedback(
	FeedbackID INT NOT NULL
);

CREATE TABLE Users (
	Email VARCHAR (100) PRIMARY KEY,
	Username VARCHAR (100) NOT NULL
);

CREATE TABLE Nieuwpoort (
	Email VARCHAR (100) PRIMARY KEY,
	FOREIGN KEY (Email) REFERENCES Users(Email)
);
