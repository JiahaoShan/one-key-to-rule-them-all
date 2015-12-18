-- activate foreign key support
PRAGMA foreign_keys = ON;

--
-- Rebuild the database, drop all tables in reverse order of creation
--

DROP TABLE IF EXISTS `Book_Rating_Relation`;
DROP TABLE IF EXISTS `Book_Author_Relation`;
DROP TABLE IF EXISTS `Book_Award_Relation`;
DROP TABLE IF EXISTS `Reader`;
DROP TABLE IF EXISTS `Award`;
DROP TABLE IF EXISTS `Book`;
DROP TABLE IF EXISTS `Publisher`;
DROP TABLE IF EXISTS `Author`;
DROP TABLE IF EXISTS `Country`;


--
-- Table structure for table `Country`
--

CREATE TABLE `Country` (
	`Code` varchar(3) NOT NULL DEFAULT '',
  `Name` varchar(35) NOT NULL DEFAULT '',
  `IsDeveloped` boolean DEFAULT '',
  PRIMARY KEY (`Code`)
);

INSERT INTO `Country` VALUES ('USA', 'United States', 1);
INSERT INTO `Country` VALUES ('TUR', 'Turkey', 0);
INSERT INTO `Country` VALUES ('CN', 'China', 0);

--
-- Table structure for table `Author`
--

CREATE TABLE `Author` (
  `ID` int(11) NOT NULL,
  `Name` varchar(64) NOT NULL DEFAULT '',
  `CountryCode` varchar(3) NOT NULL DEFAULT '',
  `Gender` varchar(6) DEFAULT '',
  `Birthday` varchar(10) DEFAULT '',
  PRIMARY KEY (`ID`),
  CONSTRAINT `author_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `Country` (`Code`)
);

INSERT INTO `Author` VALUES (0, 'Orhan Pamuk', 'TUR', 'Male', '1952-06-07');

--
-- Table structure for table `Publisher`
--

CREATE TABLE `Publisher` (
	`ID` int(11) NOT NULL,
	`Name` varchar(128) NOT NULL DEFAULT '',
	`CountryCode` varchar(3) NOT NULL DEFAULT '',
	PRIMARY KEY (`ID`),
  CONSTRAINT `publisher_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `Country` (`Code`)
);

INSERT INTO `Publisher` VALUES (0, 'Pearson', 'USA');

--
-- Table structure for table `Book`
--

CREATE TABLE `Book` (
	`ISBN` int(13) NOT NULL DEFAULT '',
	`Title` varchar(128) NOT NULL DEFAULT '',
	`TotalPages` int(5) DEFAULT '',
	`PublishDate` varchar(10) DEFAULT '',
	`Publisher` int(11) NOT NULL DEFAULT '',
	`Genre` varchar(32) DEFAULT '',
	PRIMARY KEY ('ISBN'),
	CONSTRAINT `book_ibfk_1` FOREIGN KEY (`Publisher`) REFERENCES `Publisher` (`ID`)
);

INSERT INTO `Book` VALUES (9781400078653, 'The Black Book', 100, '2014-04-05', 0, 'Fiction');

--
-- Table structure for table `Award`
--

CREATE TABLE `Award` (
	`ID` int(11) NOT NULL,
	`Name` varchar(128) NOT NULL DEFAULT '',
	`Description` varchar(128) NOT NULL DEFAULT '',
	PRIMARY KEY (`ID`)
);

INSERT INTO `Award` VALUES (0, 'Nobel Prize in Literature', 'I love this book.');

--
-- Table structure for table `Reader`
--

CREATE TABLE `Reader` (
	`ID` int(11) NOT NULL,
	`Name` varchar(128) NOT NULL DEFAULT '',
	`Email` varchar(128) DEFAULT '',
	`CountryCode` varchar(3) NOT NULL DEFAULT '',
	PRIMARY KEY (`ID`),
	CONSTRAINT `reader_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `Country` (`Code`)
);

INSERT INTO `Reader` VALUES (0, 'John Smith', 'john@example.com', 'USA');

--
-- Table structure for table `Book_Award_Relation`
--

CREATE TABLE `Book_Award_Relation` (
	`Book` int(11) NOT NULL DEFAULT '', 
	`Award` int(11) NOT NULL DEFAULT '',
	`Year` int(4) NOT NULL DEFAULT '',
	CONSTRAINT `book_award_ibfk_1` FOREIGN KEY (`Book`) REFERENCES `Book` (`ISBN`),
	CONSTRAINT `book_award_ibfk_2` FOREIGN KEY (`Award`) REFERENCES `Award` (`ID`)
);

INSERT INTO `Book_Award_Relation` VALUES (9781400078653, 0, 2006);

--
-- Table structure for table `Book_Author_Relation`
--

CREATE TABLE `Book_Author_Relation` (
	`Book` int(11) NOT NULL DEFAULT '', 
	`Author` int(11) NOT NULL DEFAULT '',
	CONSTRAINT `book_author_ibfk_1` FOREIGN KEY (`Book`) REFERENCES `Book` (`ISBN`),
	CONSTRAINT `book_author_ibfk_2` FOREIGN KEY (`Author`) REFERENCES `Author` (`ID`)
);

INSERT INTO `Book_Author_Relation` VALUES (9781400078653, 0);

--
-- Table structure for table `Book_Rating_Relation`
--

CREATE TABLE `Book_Rating_Relation` (
	`Book` int(11) NOT NULL DEFAULT '', 
	`Reader` int(11) NOT NULL DEFAULT '',
	`Rating` int(1) NOT NULL DEFAULT '',
	CONSTRAINT `book_rating_ibfk_1` FOREIGN KEY (`Book`) REFERENCES `Book` (`ISBN`),
	CONSTRAINT `book_rating_ibfk_2` FOREIGN KEY (`Reader`) REFERENCES `Reader` (`ID`)
);

INSERT INTO `Book_Rating_Relation` VALUES (9781400078653, 0, 5);




