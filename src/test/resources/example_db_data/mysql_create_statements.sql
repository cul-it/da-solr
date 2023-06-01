CREATE TABLE `replacement_headings` (
  `orig_sort` mediumtext NOT NULL,
  `preferred_display` text NOT NULL,
  KEY `orig_sort` (`orig_sort`(100))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `heading` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `parent_id` int(10) unsigned NOT NULL DEFAULT '0',
  `heading` text,
  `sort` mediumtext NOT NULL,
  `heading_type` tinyint(3) unsigned NOT NULL,
  `works_by` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `works_about` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `works` mediumint(8) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `parent_id` (`parent_id`),
  KEY `uk` (`heading_type`,`sort`(100))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `authority2heading` (
  `heading_id` int(10) unsigned NOT NULL,
  `authority_id` int(10) unsigned NOT NULL,
  `main_entry` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`heading_id`,`authority_id`),
  KEY `authority_id` (`authority_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `authority` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `source` int(1) unsigned NOT NULL,
  `nativeId` varchar(80) NOT NULL,
  `nativeHeading` text NOT NULL,
  `voyagerId` varchar(10) NOT NULL,
  `undifferentiated` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`source`,`nativeId`),
  KEY `id` (`id`),
  KEY `voyagerId` (`voyagerId`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `reference` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `from_heading` int(10) unsigned NOT NULL,
  `to_heading` int(10) unsigned NOT NULL,
  `ref_type` tinyint(3) unsigned NOT NULL,
  `ref_desc` varchar(256) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `from_heading` (`from_heading`,`to_heading`,`ref_type`,`ref_desc`),
  KEY `to_heading` (`to_heading`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `authority2reference` (
  `reference_id` int(10) unsigned NOT NULL,
  `authority_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`reference_id`,`authority_id`),
  KEY `authority_id` (`authority_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `note` (
  `heading_id` int(10) unsigned NOT NULL,
  `authority_id` int(10) unsigned NOT NULL,
  `note` text NOT NULL,
  KEY `heading_id` (`heading_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `syndeticsData` (
  `isbn` varchar(14) NOT NULL,
  `marc` mediumtext NOT NULL,
  KEY `isbn` (`isbn`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `raw_hathi` (
  `Volume_Identifier` varchar(128) NOT NULL DEFAULT '',
  `Access` text,
  `Rights` text,
  `UofM_Record_Number` varchar(128) DEFAULT NULL,
  `Enum_Chrono` text,
  `Source` varchar(12) DEFAULT NULL,
  `Source_Inst_Record_Number` varchar(1000) DEFAULT NULL,
  `OCLC_Numbers` text,
  `ISBNs` text,
  `ISSNs` text,
  `LCCNs` text,
  `Title` text,
  `Imprint` text,
  `Rights_determine_reason_code` varchar(8) DEFAULT NULL,
  `Date_Last_Update` varchar(24) DEFAULT NULL,
  `Gov_Doc` int(1) DEFAULT NULL,
  `Pub_Date` varchar(16) DEFAULT NULL,
  `Pub_Place` varchar(128) DEFAULT NULL,
  `Language` varchar(128) DEFAULT NULL,
  `Bib_Format` varchar(16) DEFAULT NULL,
  `Digitization_Agent_code` varchar(128) DEFAULT NULL,
  `Content_provider_code` varchar(128) DEFAULT NULL,
  `Responsible_Entity_code` varchar(128) DEFAULT NULL,
  `Collection_code` varchar(128) DEFAULT NULL,
  `Access_profile` varchar(512) DEFAULT NULL,
  `Author` varchar(512) DEFAULT NULL,
  `update_file_name` varchar(128) DEFAULT NULL,
  `record_counter` int(12) DEFAULT NULL,
  PRIMARY KEY (`Volume_Identifier`),
  KEY `UofM_Record_Number` (`UofM_Record_Number`),
  KEY `Author` (`Author`(333)),
  KEY `Access_profile` (`Access_profile`(333)),
  KEY `Local_Identifiers` (`Source`,`Source_Inst_Record_Number`(12)),
  KEY `Source_Inst_Record_Number_idx` (`Source_Inst_Record_Number`(333))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `volume_to_oclc` (
  `Volume_Identifier` varchar(128) DEFAULT NULL,
  `OCLC_Number` varchar(250) DEFAULT NULL,
  KEY `Volume_Identifier` (`Volume_Identifier`),
  KEY `OCLC_Number` (`OCLC_Number`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `volume_to_source_inst_rec_num` (
  `Volume_Identifier` varchar(128) DEFAULT NULL,
  `Source_Inst_Record_Number` varchar(256) DEFAULT NULL,
  KEY `Volume_Identifier` (`Volume_Identifier`),
  KEY `Source_Inst_Record_Number` (`Source_Inst_Record_Number`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `classification` (
  `low_letters` char(3) NOT NULL,
  `high_letters` char(3) NOT NULL,
  `low_numbers` float(10,4) NOT NULL,
  `high_numbers` float(10,4) NOT NULL,
  `label` varchar(256) CHARACTER SET utf8 NOT NULL,
  KEY `low_letters` (`low_letters`,`high_letters`,`low_numbers`,`high_numbers`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

