CREATE TABLE `cohorts_daily` (
  `date` date NOT NULL,
  `client` varchar(16) NOT NULL,
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
  `client` varchar(16) NOT NULL,
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
  `client` varchar(16) NOT NULL,
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


-- add an `sso_idp` column to each of the cohort tables, so that we can break down
-- retention by IDP. In order function correctly as part of a unique key, it needs to
-- be non-nullable, since mysql treats (null = null) as false.

ALTER TABLE `cohorts_daily`
   ADD COLUMN `sso_idp` varchar(16) NOT NULL DEFAULT '',
   ADD UNIQUE KEY `client_idp_date` (`client`,`sso_idp`, `date`),
   DROP KEY `client_date`;

ALTER TABLE `cohorts_weekly`
   ADD COLUMN `sso_idp` varchar(16) NOT NULL DEFAULT '',
   ADD UNIQUE KEY `client_idp_date` (`client`,`sso_idp`, `date`),
   DROP KEY `client_date`;

ALTER TABLE `cohorts_monthly`
   ADD COLUMN `sso_idp` varchar(16) NOT NULL DEFAULT '',
   ADD UNIQUE KEY `client_idp_date` (`client`,`sso_idp`, `date`),
   DROP KEY `client_date`;


-- add a `cohort_size` column to each of the cohort tables, so that we can track
-- the size of the entire cohort (and therefore give relative retention in %).

ALTER TABLE `cohorts_daily`
  ADD COLUMN `cohort_size` int(11);

ALTER TABLE `cohorts_weekly`
  ADD COLUMN `cohort_size` int(11);

ALTER TABLE `cohorts_monthly`
  ADD COLUMN `cohort_size` int(11);
