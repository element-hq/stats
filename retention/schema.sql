CREATE TABLE `cohorts_daily` (
  `date` date NOT NULL,
  `client` varchar(12) NOT NULL,
  `b1` int(11) NOT NULL DEFAULT '0',
  `b2` int(11) NOT NULL DEFAULT '0',
  `b3` int(11) NOT NULL DEFAULT '0',
  `b4` int(11) NOT NULL DEFAULT '0',
  `b5` int(11) NOT NULL DEFAULT '0',
  `b6` int(11) NOT NULL DEFAULT '0',
  `b7` int(11) NOT NULL DEFAULT '0',
  `b8` int(11) NOT NULL DEFAULT '0',
  `b9` int(11) NOT NULL DEFAULT '0',
  `b10` int(11) NOT NULL DEFAULT '0',
  `b11` int(11) NOT NULL DEFAULT '0',
  `b12` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `client_date` (`client`,`date`)
) ENGINE=InnoDB;

CREATE TABLE `cohorts_weekly` (
  `date` date NOT NULL,
  `client` varchar(12) NOT NULL,
  `b1` int(11) NOT NULL DEFAULT '0',
  `b2` int(11) NOT NULL DEFAULT '0',
  `b3` int(11) NOT NULL DEFAULT '0',
  `b4` int(11) NOT NULL DEFAULT '0',
  `b5` int(11) NOT NULL DEFAULT '0',
  `b6` int(11) NOT NULL DEFAULT '0',
  `b7` int(11) NOT NULL DEFAULT '0',
  `b8` int(11) NOT NULL DEFAULT '0',
  `b9` int(11) NOT NULL DEFAULT '0',
  `b10` int(11) NOT NULL DEFAULT '0',
  `b11` int(11) NOT NULL DEFAULT '0',
  `b12` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `client_date` (`client`,`date`)
) ENGINE=InnoDB;

CREATE TABLE `cohorts_monthly` (
  `date` date NOT NULL,
  `client` varchar(12) NOT NULL,
  `b1` int(11) NOT NULL DEFAULT '0',
  `b2` int(11) NOT NULL DEFAULT '0',
  `b3` int(11) NOT NULL DEFAULT '0',
  `b4` int(11) NOT NULL DEFAULT '0',
  `b5` int(11) NOT NULL DEFAULT '0',
  `b6` int(11) NOT NULL DEFAULT '0',
  `b7` int(11) NOT NULL DEFAULT '0',
  `b8` int(11) NOT NULL DEFAULT '0',
  `b9` int(11) NOT NULL DEFAULT '0',
  `b10` int(11) NOT NULL DEFAULT '0',
  `b11` int(11) NOT NULL DEFAULT '0',
  `b12` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `client_date` (`client`,`date`)
) ENGINE=InnoDB
