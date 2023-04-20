CREATE TABLE Kiten (
					   DataID VARCHAR (100) PRIMARY KEY,
					   Loc VARCHAR (100) NOT NULL,
					   Windspeed VARCHAR (100) NOT NULL,
					   WindspeedUnit VARCHAR (100) NOT NULL,
					   Waveheight VARCHAR (100) NOT NULL,
					   WaveheightUnit VARCHAR (100) NOT NULL,
					   Winddirection VARCHAR (100) NOT NULL,
					   WinddirectionUnit VARCHAR (100) NOT NULL,
					   TimestampMeasurment INTEGER NOT NULL
);

CREATE TABLE Feedback(
						 FeedbackID VARCHAR(100) PRIMARY KEY,
						 Loc VARCHAR(100) NOT NULL,
						 Username VARCHAR(100) NOT NULL,
						 Feedback VARCHAR(256) NOT NULL,
						 TimestampFeedback INTEGER NOT NULL
);

CREATE TABLE Users (
					   Email VARCHAR (255) PRIMARY KEY,
					   Username VARCHAR (100) NOT NULL
);

CREATE TABLE keepUpdated (
							 Location VARCHAR(100) NOT NULL,
							 Email VARCHAR (100) PRIMARY KEY,
							 FOREIGN KEY (Email) REFERENCES Users(Email)
);

INSERT INTO Users(Email, Username) VALUES ('joran.vanbelle2@student.hogent.be', 'Joran');
INSERT INTO keepUpdated(Location, Email) VALUES('Nieuwpoort', 'joran.vanbelle2@student.hogent.be');
INSERT INTO Feedback(FeedbackID, Loc, Username, Feedback, TimestampFeedback) VALUES ('JoranNieuwpoort1', 'Nieuwpoort', 'Joran', 'The waves are amazing', 1);
INSERT INTO Feedback(FeedbackID, Loc, Username, Feedback, TimestampFeedback) VALUES ('JoranDe Panne1', 'De Panne', 'Joran', 'There is too little wind here', 1);