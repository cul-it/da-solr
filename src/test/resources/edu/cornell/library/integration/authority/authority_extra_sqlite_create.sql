CREATE TABLE IF NOT EXISTS `madsAuthorityUpdate` (`id` text DEFAULT NULL, `lccn` text DEFAULT NULL, `vocabulary` int DEFAULT NULL, `recordStatus` int DEFAULT NULL, `heading` text DEFAULT NULL, `headingType` int DEFAULT NULL, `isSubdivision` int DEFAULT NULL, `undifferentiated` int DEFAULT NULL, `moddate` text DEFAULT NULL, `addedDate` text DEFAULT NULL, `numUpdates` int DEFAULT 0, source text NOT NULL, PRIMARY KEY (`id`, `numUpdates`, `moddate`));
CREATE TABLE IF NOT EXISTS `madsAuthorityHeadingType` (`id` int NOT NULL, `name` VARCHAR(256) NOT NULL, PRIMARY KEY (`id`));
CREATE TABLE IF NOT EXISTS `madsAuthorityRecordStatus` (`id` int NOT NULL, `name` VARCHAR(256) NOT NULL, PRIMARY KEY (`id`));
-- headings
CREATE TABLE `madsAuthority_source` (`id` int NOT NULL, `name` text NOT NULL);
CREATE TABLE `madsAuthority` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `source` integer NOT NULL, `nativeId` varchar(80) NOT NULL, `nativeHeading` text NOT NULL, `localId` varchar(10) NOT NULL, `undifferentiated` integer NOT NULL DEFAULT '0');
CREATE TABLE `madsAuthority2heading` (`heading_id` integer NOT NULL, `authority_id` integer NOT NULL, `main_entry` integer NOT NULL DEFAULT '0', PRIMARY KEY (`heading_id`,`authority_id`));
CREATE TABLE `madsAuthority2reference` (`reference_id` integer NOT NULL, `authority_id` integer NOT NULL, PRIMARY KEY (`reference_id`,`authority_id`));
CREATE TABLE `madsBib2heading` (`bib_id` int NOT NULL, `category` int NOT NULL, `heading_id` int NOT NULL, `heading` text);
CREATE TABLE `madsHeading_category` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `madsHeading_type` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `madsHeading` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `parent_id` integer NOT NULL DEFAULT '0', `heading` text, `sort` mediumtext NOT NULL, `heading_type` integer NOT NULL, `works_by` integer NOT NULL DEFAULT '0', `works_about` integer NOT NULL DEFAULT '0', `works` integer NOT NULL DEFAULT '0');
CREATE TABLE `madsNote` (`heading_id` integer NOT NULL, `authority_id` integer NOT NULL, `note` text NOT NULL);
CREATE TABLE `madsRda` (`heading_id` int NOT NULL, `authority_id` int NOT NULL, `rda` text NOT NULL);
CREATE TABLE `madsReferenceType` (`id` int NOT NULL, `name` text NOT NULL, PRIMARY KEY  (`id`));
CREATE TABLE `madsReference` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `from_heading` integer NOT NULL, `to_heading` integer NOT NULL, `ref_type` integer NOT NULL, `ref_desc` varchar(256) NOT NULL DEFAULT '', UNIQUE (`from_heading`,`to_heading`,`ref_type`,`ref_desc`));
-- CREATE TABLE `replacement_headings` (`orig_sort` mediumtext NOT NULL, `preferred_display` text NOT NULL);
-- CREATE TABLE `headingsUpdateCursor` (`cursor_name` text NOT NULL, `current_to_date` text DEFAULT NULL, PRIMARY KEY (`cursor_name`));