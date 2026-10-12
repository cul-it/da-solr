-- authority
CREATE TABLE `authoritySource` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY (`id`));
CREATE TABLE `authorityUpdate` (`id` text DEFAULT NULL, `vocabulary` int DEFAULT NULL, `updateFile` text NOT NULL DEFAULT '', `positionInFile` int NOT NULL DEFAULT 0, `changeType` int DEFAULT NULL, `heading` text DEFAULT NULL, `headingType` int DEFAULT NULL, `linkedSubdivision` text DEFAULT NULL, `undifferentiated` int DEFAULT NULL, `marc21` text DEFAULT NULL, `human` text DEFAULT NULL, `moddate` date DEFAULT NULL, PRIMARY KEY (`updateFile`,`positionInFile`));
CREATE TABLE `changeType` (`id` int unsigned NOT NULL, `name` text NOT NULL, PRIMARY KEY (`id`));
CREATE TABLE `current` (`id` text NOT NULL, `heading` text DEFAULT NULL, `updateFile` text DEFAULT NULL, `moddate` date NOT NULL, `undifferentiated` int DEFAULT 0, PRIMARY KEY (`id`));
CREATE TABLE `headingType` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY (`id`));
CREATE TABLE `lcAuthorityFile` (`updateFile` text NOT NULL, `postedDate` timestamp NULL DEFAULT NULL, `importedDate` timestamp NULL DEFAULT NULL, `reportedDate` timestamp NULL DEFAULT NULL, PRIMARY KEY (`updateFile`));
-- sqlite doesn't support on update clause for timestamp and we will need to add trigger if we want to add that functionality
CREATE TABLE `updateCursor` (`cursor_name` text NOT NULL, `current_to_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`cursor_name`));
CREATE TABLE `voyagerAuthority` (`id` text NOT NULL, `voyId` int NOT NULL, `marc21` text DEFAULT NULL, `marcxml` text DEFAULT NULL, `human` text DEFAULT NULL, `moddate` datetime DEFAULT NULL);
-- headings
CREATE TABLE `authority_source` (`id` int NOT NULL, `name` text NOT NULL);
CREATE TABLE `authority` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `source` integer NOT NULL, `nativeId` varchar(80) NOT NULL, `nativeHeading` text NOT NULL, `localId` varchar(10) NOT NULL, `undifferentiated` integer NOT NULL DEFAULT '0');
CREATE TABLE `authority2heading` (`heading_id` integer NOT NULL, `authority_id` integer NOT NULL, `main_entry` integer NOT NULL DEFAULT '0', PRIMARY KEY (`heading_id`,`authority_id`));
CREATE TABLE `authority2reference` (`reference_id` integer NOT NULL, `authority_id` integer NOT NULL, PRIMARY KEY (`reference_id`,`authority_id`));
CREATE TABLE `bib2heading` (`bib_id` int NOT NULL, `category` int NOT NULL, `heading_id` int NOT NULL, `heading` text);
CREATE TABLE `heading_category` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `heading_type` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `heading` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `parent_id` integer NOT NULL DEFAULT '0', `heading` text, `sort` mediumtext NOT NULL, `heading_type` integer NOT NULL, `works_by` integer NOT NULL DEFAULT '0', `works_about` integer NOT NULL DEFAULT '0', `works` integer NOT NULL DEFAULT '0');
CREATE TABLE `note` (`heading_id` integer NOT NULL, `authority_id` integer NOT NULL, `note` text NOT NULL);
CREATE TABLE `raw_hathi` (`Volume_Identifier` varchar(128) NOT NULL DEFAULT '', `Access` text, `Rights` text, `UofM_Record_Number` varchar(128) DEFAULT NULL, `Enum_Chrono` text, `Source` varchar(12) DEFAULT NULL collate nocase, `Source_Inst_Record_Number` varchar(1000) DEFAULT NULL, `OCLC_Numbers` text, `ISBNs` text, `ISSNs` text, `LCCNs` text, `Title` text, `Imprint` text, `Rights_determine_reason_code` varchar(8) DEFAULT NULL, `Date_Last_Update` varchar(24) DEFAULT NULL, `Gov_Doc` integer DEFAULT NULL, `Pub_Date` varchar(16) DEFAULT NULL, `Pub_Place` varchar(128) DEFAULT NULL, `Language` varchar(128) DEFAULT NULL, `Bib_Format` varchar(16) DEFAULT NULL, `Digitization_Agent_code` varchar(128) DEFAULT NULL, `Content_provider_code` varchar(128) DEFAULT NULL, `Responsible_Entity_code` varchar(128) DEFAULT NULL, `Collection_code` varchar(128) DEFAULT NULL, `Access_profile` varchar(512) DEFAULT NULL, `Author` varchar(512) DEFAULT NULL, `update_file_name` varchar(128) DEFAULT NULL, `record_counter` integer DEFAULT NULL, PRIMARY KEY (`Volume_Identifier`));
CREATE TABLE `rda` (`heading_id` int NOT NULL, `authority_id` int NOT NULL, `rda` text NOT NULL);
CREATE TABLE `ref_type` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `reference` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `from_heading` integer NOT NULL, `to_heading` integer NOT NULL, `ref_type` integer NOT NULL, `ref_desc` varchar(256) NOT NULL DEFAULT '', UNIQUE (`from_heading`,`to_heading`,`ref_type`,`ref_desc`));
CREATE TABLE `replacement_headings` (`orig_sort` mediumtext NOT NULL, `preferred_display` text NOT NULL);
CREATE TABLE `headingsUpdateCursor` (`cursor_name` text NOT NULL, `current_to_date` date DEFAULT NULL, PRIMARY KEY (`cursor_name`));
-- classifications
CREATE TABLE `classification` (`low_letters` char(3) NOT NULL, `high_letters` char(3) NOT NULL, `low_numbers` float(10,4) NOT NULL, `high_numbers` float(10,4) NOT NULL, `label` varchar(256) NOT NULL);
-- current
CREATE TABLE `syndeticsData` (`isbn` varchar(14) NOT NULL, `marc` mediumtext NOT NULL);
-- Hathi
CREATE TABLE `volume_to_oclc` (`Volume_Identifier` varchar(128) DEFAULT NULL, `OCLC_Number` varchar(250) DEFAULT NULL);
CREATE TABLE `volume_to_source_inst_rec_num` (`Volume_Identifier` varchar(128) DEFAULT NULL, `Source_Inst_Record_Number` varchar(256) DEFAULT NULL);
-- index
-- authority
CREATE INDEX "idx_authorityUpdate_id" ON "authorityUpdate" (`id`);
CREATE INDEX "idx_moddate" ON "authorityUpdate" (`moddate`);
CREATE INDEX "idx_voyagerAuthority_id" ON "voyagerAuthority" (`id`);
CREATE INDEX "idx_voyagerAuthority_voyid" ON "voyagerAuthority" (`voyid`);
-- headings
CREATE INDEX "idx_authority_source_id" ON "authority_source" (`id`);
CREATE INDEX "idx_authority_id" ON "authority" (`id`);
CREATE INDEX "idx_authority_localId" ON "authority" (`localId`);
CREATE INDEX "idx_authority2heading_authority_id" ON "authority2heading" (`authority_id`);
CREATE INDEX "idx_authority2reference_authority_id" ON "authority2reference" (`authority_id`);
CREATE INDEX "idx_bib_id" on "bib2heading" (`bib_id`);
CREATE INDEX "idx_bib2heading_heading_id" ON "bib2heading" (`heading_id`);
CREATE INDEX "idx_rda_heading_id" ON "rda" (`heading_id`);
CREATE INDEX "idx_heading_parent_id" ON "heading" (`parent_id`);
CREATE INDEX "idx_heading_uk" ON "heading" (`heading_type`,`sort`);
CREATE INDEX "idx_note_heading_id" ON "note" (`heading_id`);
CREATE INDEX "idx_raw_hathi_Access_profile" ON "raw_hathi" (`Access_profile`);
CREATE INDEX "idx_raw_hathi_Author" ON "raw_hathi" (`Author`);
CREATE INDEX "idx_raw_hathi_Local_Identifiers" ON "raw_hathi" (`Source`,`Source_Inst_Record_Number`);
CREATE INDEX "idx_raw_hathi_Source_Inst_Record_Number_idx" ON "raw_hathi" (`Source_Inst_Record_Number`);
CREATE INDEX "idx_raw_hathi_UofM_Record_Number" ON "raw_hathi" (`UofM_Record_Number`);
CREATE INDEX "idx_reference_to_heading" ON "reference" (`to_heading`);
CREATE INDEX "idx_replacement_headings_orig_sort" ON "replacement_headings" (`orig_sort`);
CREATE UNIQUE INDEX "uidx_category" on "bib2heading" (`category`,`heading_id`,`bib_id`);
-- classifications
CREATE INDEX "idx_classification_low_letters" ON "classification" (`low_letters` collate nocase,`high_letters` collate nocase,`low_numbers`,`high_numbers`);
-- current
CREATE INDEX "idx_syndeticsData_isbn" ON "syndeticsData" (`isbn`);
-- Hathi
CREATE INDEX "idx_volume_to_oclc_OCLC_Number" ON "volume_to_oclc" (`OCLC_Number`);
CREATE INDEX "idx_volume_to_oclc_Volume_Identifier" ON "volume_to_oclc" (`Volume_Identifier`);
CREATE INDEX "idx_volume_to_source_inst_rec_num_Source_Inst_Record_Number" ON "volume_to_source_inst_rec_num" (`Source_Inst_Record_Number`);
CREATE INDEX "idx_volume_to_source_inst_rec_num_Volume_Identifier" ON "volume_to_source_inst_rec_num" (`Volume_Identifier`);
