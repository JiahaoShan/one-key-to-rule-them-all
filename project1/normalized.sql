-- activate foreign key support
PRAGMA foreign_keys = ON;

-- drop existing tables in reverse order
DROP TABLE IF EXISTS `Sales`;
DROP TABLE IF EXISTS `Attributes`;
DROP TABLE IF EXISTS `WeekDate`;
DROP TABLE IF EXISTS `Store`;

--
-- Table structure for table `Store`
--

CREATE TABLE `Store` (
	`Store` INTEGER NOT NULL DEFAULT '',
  `Size` INTEGER NOT NULL DEFAULT '',
  `Type` CHAR(1) NOT NULL DEFAULT '',
  PRIMARY KEY (`Store`)
);

--
-- Table structure for table `WeekDate`
--

CREATE TABLE `WeekDate` (
	`WeekDate` DATE NOT NULL DEFAULT '',
  `IsHoliday` BOOLEAN NOT NULL DEFAULT '',
  PRIMARY KEY (`WeekDate`)
);

--
-- Table structure for table `Attributes`
--

CREATE TABLE `Attributes` (
	`Store`	INTEGER NOT NULL DEFAULT '',
	`WeekDate` DATE NOT NULL DEFAULT '',
  `Temperature` BOOLEAN(1) NOT NULL DEFAULT '',
  `FuelPrice` REAL NOT NULL DEFAULT '',
  `CPI` REAL NOT NULL DEFAULT '',
  `UnemploymentRate` REAL NOT NULL DEFAULT '',
  PRIMARY KEY (`Store`, `WeekDate`),
  CONSTRAINT `attributes_ibfk_1` FOREIGN KEY (`Store`) REFERENCES `Store` (`Store`),
  CONSTRAINT `attributes_ibfk_2` FOREIGN KEY (`WeekDate`) REFERENCES `WeekDate` (`WeekDate`)
);

--
-- Table structure for table `Sales`
--

CREATE TABLE `Sales` (
	`Store` INTEGER NOT NULL DEFAULT '',
  `WeekDate` DATE NOT NULL DEFAULT '',
  `Dept` INTEGER NOT NULL DEFAULT '',
  `WeeklySales` REAL NOT NULL DEFAULT '',
  PRIMARY KEY (`Store`, `WeekDate`, `Dept`),
  CONSTRAINT `sales_ibfk_1` FOREIGN KEY (`Store`) REFERENCES `Store` (`Store`),
  CONSTRAINT `sales_ibfk_2` FOREIGN KEY (`WeekDate`) REFERENCES `WeekDate` (`WeekDate`)
);
