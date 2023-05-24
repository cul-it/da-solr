CREATE TABLE `replacement_headings` (
  `orig_sort` mediumtext NOT NULL,
  `preferred_display` text NOT NULL,
  KEY `orig_sort` (`orig_sort`(100))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("illegal aliens", "Undocumented immigrants");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("children of illegal aliens", "Children of undocumented immigrants");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("illegal alien children", "Undocumented immigrant children");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("illegal aliens in literature", "Undocumented immigrants in literature");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("women illegal aliens", "Women undocumented immigrants");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("alien detention centers", "Immigrant detection centers");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("aliens", "Noncitizens");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("illegal immigration", "Undocumented immigratio");
INSERT INTO replacement_headings (orig_sort, preferred_display) VALUES ("illegal immigration in literature", "Undocumented immigration in literature");


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

-- Heading2SolrTest data

INSERT INTO note (heading_id, authority_id, note) VALUES (296895, 1440605, "Search under: subdivision Freemasonry under names of persons");
INSERT INTO note (heading_id, authority_id, note) VALUES (1537075, 742440, "Search under: headings beginning with the words Mine and Mining");
INSERT INTO note (heading_id, authority_id, note) VALUES (1537075, 742440, "Search under: subdivision Effect of mining on under individual animals and groups of animals, e.g. Fishes--Effect of mining on");
INSERT INTO note (heading_id, authority_id, note) VALUES (4496, 2327, '["For works of this author entered under other names, search also under",{"header":"Grandower, Elissa"},{"header":"Taylor, H. Baldwin"}]');

-- AuthorTitleTest data
INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (10422460, 0, "n 2017073745", "León Cupe, Mariano, 1932-", 10721247, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (18642914, 0, "León Cupe, Mariano, 1932-", "leon cupe mariano 1932", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (18642915, 0, "Cupe, Mariano León, 1932-", "cupe mariano leon 1932", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (18642914, 10422460, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (9187707, 18642915, 18642914, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (9187707, 10422460);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (10046868, 0, "no2016116176", "Papadēmētropoulos, Loukas P.", 10342895, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (18015372, 0, "Papadēmētropoulos, Loukas P.", "papademetropoulos loukas p", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (18015372, 10046868, 1);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (284161, 0, "n  83060502", "Foucher, A. (Alfred), 1865-1952", 286151, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (640475, 0, "Foucher, A. (Alfred), 1865-1952", "foucher a alfred 1865 1952", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (640476, 0, "Foucher, Alfred Charles Auguste, 1865-1952", "foucher alfred charles auguste 1865 1952", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (640475, 284161, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (368603, 640476, 640475, 1, "Later Form of Heading");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (368603, 284161);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (5302631, 0, "no2001043347", "Taga, Futoshi, 1968-", 5502637, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10141115, 0, "Taga, Futoshi, 1968-", "taga futoshi 1968", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10141116, 0, "多賀太, 1968-", "多賀太 1968", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (10141115, 5302631, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5370664, 10141116, 10141115, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5370664, 5302631);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (999994, 0, "n  81062107", "Guo li gu gong bo wu yuan", 1033816, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (376305, 0, "Guo li zhong yang bo wu yuan", "guo li zhong yang bo wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (376318, 0, "Guo li gu gong bo wu yuan", "guo li gu gong bo wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255513, 0, "Gu gong yuan (Taipei, Taiwan)", "gu gong yuan taipei taiwan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255514, 0, "China (Republic : 1949- ). Guo li gu gong bo wu yuan", "china republic 1949 guo li gu gong bo wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255515, 0, "China (Republic : 1949- ). Chinese National Palace Museum", "china republic 1949 chinese national palace museum", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255516, 0, "Taipei (Taiwan). Chinese National Palace Museum", "taipei taiwan chinese national palace museum", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255517, 0, "Chūka Minkoku Kokuritsu Kokyū Hakubutsuin", "chuka minkoku kokuritsu kokyu hakubutsuin", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255518, 0, "Taipei (Taiwan). Guo li gu gong bo wu yuan", "taipei taiwan guo li gu gong bo wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255519, 0, "Kokuritsu Kokyū Hakubutsuin", "kokuritsu kokyu hakubutsuin", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255520, 0, "Musée national du Palais (Taipei, Taiwan)", "musee national du palais taipei taiwan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255521, 0, "Taibei gu gong bo wu yuan", "taibei gu gong bo wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255522, 0, "Kuo li ku kung po wu yüan", "kuo li ku kung po wu yuan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255523, 0, "Gu gong bo wu yuan (Taipei, Taiwan)", "gu gong bo wu yuan taipei taiwan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255524, 0, "故宮博物院 (Taipei, Taiwan)", "故宮博物院 taipei taiwan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255525, 0, "國立故宮博物院", "國立故宮博物院", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255526, 0, "China (Republic : 1949- ). 國立故宮博物院", "china republic 1949 國立故宮博物院", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255527, 0, "National Palace Museum (Taipei, Taiwan)", "national palace museum taipei taiwan", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255528, 0, "China (Republic : 1949- ). National Palace Museum", "china republic 1949 national palace museum", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255529, 0, "Chinese National Palace Museum", "chinese national palace museum", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255530, 0, "台北 (台灣). 國立故宮博物院", "台北 台灣 國立故宮博物院", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2255531, 0, "台北故宮博物院", "台北故宮博物院", 1, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (376318, 172626, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (376318, 269124, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (376318, 999994, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462865, 2255513, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462866, 2255514, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462867, 2255515, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462868, 2255516, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462869, 2255517, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462870, 2255518, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462871, 2255519, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462872, 2255520, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462873, 2255521, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462874, 2255522, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462875, 2255523, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462876, 2255524, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462877, 2255525, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462878, 2255526, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462879, 2255527, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462880, 2255528, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462881, 2255529, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462882, 2255530, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462883, 2255531, 376318, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1462884, 376305, 376318, 3, "Later Heading");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462865, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462866, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462867, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462868, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462869, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462870, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462871, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462872, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462873, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462874, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462875, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462876, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462877, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462878, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462879, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462880, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462881, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462882, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462883, 999994);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1462884, 999994);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (1396245, 0, "n  99010492", "Fewer, T. N.", 1451941, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (3231839, 0, "Fewer, T. N.", "fewer t n", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (3231840, 0, "Fewer, Tom", "fewer tom", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (3231839, 1396245, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (2083972, 3231840, 3231839, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (2083972, 1396245);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (4017405, 0, "nr 96008001", "Korea (South). President (1993-1998 : Kim)", 4195887, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2256520, 0, "Kim, Young Sam, 1927-2015", "kim young sam 1927 2015", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2256526, 0, "Korea (South). President (1993-1998 : Kim)", "korea south president 1993 1998 kim", 1, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (2256526, 1000403, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (2256526, 4017405, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (4290879, 2256520, 2256526, 3, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (4290879, 4017405);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (72775, 0, "n  80038440", "Speed, John, 1552?-1629", 73505, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142720, 0, "Speed, John, 1552?-1629", "speed john 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142721, 0, "J. S. (John Speed), 1552?-1629", "j s john speed 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142722, 0, "Speed, I. (John Speed), 1552?-1629", "speed i john speed 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142723, 0, "I. S. (John Speed), 1552?-1629", "i s john speed 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142724, 0, "Speed, Iohn, 1552?-1629", "speed iohn 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142725, 0, "S., I. (John Speed), 1552?-1629", "s i john speed 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142726, 0, "Speede, Iohn, 1552?-1629", "speede iohn 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142727, 0, "Spede, Iohn, 1552?-1629", "spede iohn 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142728, 0, "Speede, John, 1552?-1629", "speede john 1552 1629", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (142729, 0, "S., J. (John Speed), 1552?-1629", "s j john speed 1552 1629", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (142720, 72775, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72021, 142721, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72022, 142722, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72023, 142723, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72024, 142724, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72025, 142725, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72026, 142726, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72027, 142727, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72028, 142728, 142720, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (72029, 142729, 142720, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72021, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72022, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72023, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72024, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72025, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72026, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72027, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72028, 72775);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (72029, 72775);


-- SubjectTest data
INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (1043886, 0, "n  94100484", "Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). Decree Four", 1078612, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2364788, 2364787, "Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four", "jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four", 9, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2364789, 2364787, "Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Our mission today", "jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 our mission today", 9, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2364790, 2364787, "Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Jesuits today", "jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 jesuits today", 9, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (2364788, 1043886, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1534029, 2364789, 2364788, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1534030, 2364790, 2364788, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1534029, 1043886);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1534030, 1043886);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (812808, 1, "sh 85003553", "Illegal aliens", 837139, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1534531, 0, "Aliens", "aliens", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825884, 0, "Aliens > Legal status, laws, etc.", "aliens 0000 legal status laws etc", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825889, 0, "Illegal aliens", "illegal aliens", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825890, 0, "Aliens, Illegal", "aliens illegal", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825891, 0, "Undocumented aliens", "undocumented aliens", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825892, 0, "Illegal aliens > Legal status, laws, etc.", "illegal aliens 0000 legal status laws etc", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825893, 0, "Illegal immigrants", "illegal immigrants", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825894, 0, "Illegal immigration", "illegal immigration", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825895, 0, "Human smuggling", "human smuggling", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1825896, 0, "Alien detention centers", "alien detention centers", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1825889, 812808, 1);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1825889, 1247779, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1825889, 5501744, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1825889, 5910376, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1825889, 8436996, 0);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161504, 1825890, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161505, 1825884, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161506, 1825891, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161507, 1825892, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161508, 1825893, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161509, 1825894, 1825889, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161510, 1534531, 1825889, 3, "Narrower Term");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161511, 1825895, 1825889, 3, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1161512, 1825896, 1825889, 3, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161504, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161505, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161506, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161507, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161508, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161509, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161510, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161511, 812808);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1161512, 812808);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (7394879, 1, "sh2008002554", "Illegal alien children", 7652978, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1540026, 0, "Children", "children", 4, 0, 1, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (13650791, 0, "Illegal alien children", "illegal alien children", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (13650792, 0, "Undocumented children", "undocumented children", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (13650791, 7394879, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (6963118, 13650792, 13650791, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (6963119, 1540026, 13650791, 3, "Narrower Term");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (6963118, 7394879);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (6963119, 7394879);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (702299, 1, "sh 85042790", "Emigration and immigration law", 717360, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1530221, 0, "International travel regulations", "international travel regulations", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1530222, 0, "Emigration and immigration law", "emigration and immigration law", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1601026, 0, "Emigration and immigration > Law and legislation", "emigration and immigration 0000 law and legislation", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1601027, 0, "Law, Immigration", "law immigration", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1601028, 0, "Immigration law", "immigration law", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1601029, 0, "Immigrants > Legal status, laws, etc.", "immigrants 0000 legal status laws etc", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1601030, 0, "Law, Emigration", "law emigration", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 673372, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 697682, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 702299, 1);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 705224, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 709372, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 719512, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 719513, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 739529, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 757506, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 797554, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1530222, 7719225, 0);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941367, 1601026, 1530222, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941368, 1601027, 1530222, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941369, 1601028, 1530222, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941370, 1601029, 1530222, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941371, 1601030, 1530222, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (941372, 1530221, 1530222, 3, "Narrower Term");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941367, 702299);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941368, 702299);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941369, 702299);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941370, 702299);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941371, 702299);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (941372, 702299);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (818337, 1, "sh 85042791", "Emigration and immigration law > United States", 843208, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1839204, 0, "Emigration and immigration law > United States", "emigration and immigration law 0000 united states", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1839205, 0, "United States > Emigration and immigration law", "united states 0000 emigration and immigration law", 5, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1839204, 818337, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1175029, 1839205, 1839204, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1175029, 818337);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (750825, 1, "sh 85104440", "Political science", 769920, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1530169, 0, "Administration", "administration", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1544055, 0, "Political science", "political science", 4, 0, 1, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1571731, 0, "State, The", "state the", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1576763, 0, "Social sciences", "social sciences", 4, 0, 1, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704499, 0, "Government", "government", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704500, 0, "Politics", "politics", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704501, 0, "Science, Political", "science political", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704502, 0, "Political theory", "political theory", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704503, 0, "Commonwealth, The", "commonwealth the", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704504, 0, "Civil government", "civil government", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1704505, 0, "Political thought", "political thought", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 678665, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 679089, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 683861, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 689037, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 689688, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 689789, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 689805, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 691925, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 692105, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 693091, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 697014, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 697246, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 701267, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 703711, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 705146, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 706128, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 706611, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 711710, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 719607, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 720980, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 721759, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 726100, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 728839, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 735621, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 736037, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 739418, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 743166, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 747133, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750082, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750626, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750701, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750768, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750800, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750810, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750820, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750825, 1);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 750843, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 751193, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 751915, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 752359, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 753981, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 754076, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 755506, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757266, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757559, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757569, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757570, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757837, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 757997, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 758452, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 763196, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 766252, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 767524, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 768859, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 768901, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 771895, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 773447, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 776492, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 779713, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 780886, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 784542, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 791196, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 793567, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 793943, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 817921, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 819150, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 821378, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 833324, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 834678, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 1326609, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 1438263, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 5674644, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 6267545, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 6499227, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 6850639, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 7092246, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 7164872, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 7204248, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1544055, 10135252, 0);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042606, 1704499, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042607, 1704500, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042608, 1704501, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042609, 1704502, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042610, 1704503, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042611, 1530169, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042612, 1704504, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042613, 1704505, 1544055, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042614, 1571731, 1544055, 3, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1042615, 1576763, 1544055, 3, "Narrower Term");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042606, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042607, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042608, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042609, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042610, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042611, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042612, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042613, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042614, 750825);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1042615, 750825);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (770611, 1, "sh 85129516", "Submerged lands", 791488, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1563559, 0, "Land use", "land use", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1674459, 0, "Submerged lands", "submerged lands", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1743287, 0, "Lands under the marginal sea", "lands under the marginal sea", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1743288, 0, "Tidelands", "tidelands", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1743289, 0, "Lands beneath navigable waters", "lands beneath navigable waters", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1743290, 0, "Submerged coastal lands", "submerged coastal lands", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1674459, 735903, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1674459, 770611, 1);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1674459, 774913, 0);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1082755, 1743287, 1674459, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1082756, 1743288, 1674459, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1082757, 1743289, 1674459, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1082758, 1743290, 1674459, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1082759, 1563559, 1674459, 3, "Narrower Term");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1082755, 770611);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1082756, 770611);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1082757, 770611);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1082758, 770611);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1082759, 770611);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (1127010, 0, "n  80014970", "Cambodia", 1167533, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1183744, 0, "French Indochina", "french indochina", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1397094, 0, "Cambodia", "cambodia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567587, 0, "République du Cambodge", "republique du cambodge", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567588, 0, "Kambodža", "kambodza", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567589, 0, "Kambujā", "kambuja", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567590, 0, "Kampuchii︠a︡", "kampuchiia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567591, 0, "Roat Kampuchea", "roat kampuchea", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567592, 0, "Khmer Republic", "khmer republic", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567593, 0, "Preah Reach Ana Chak Kampuchea", "preah reach ana chak kampuchea", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567594, 0, "Camboja", "camboja", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567595, 0, "Cambodia. Rājraṭṭhabhipāl Kambujā", "cambodia rajratthabhipal kambuja", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567596, 0, "Cambodia. Royal Government of Cambodia", "cambodia royal government of cambodia", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567597, 0, "Kampuchea démocratique", "kampuchea democratique", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567598, 0, "Cambodge", "cambodge", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567599, 0, "Tchin-la", "tchin la", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567600, 0, "Campuchia", "campuchia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567601, 0, "State of Cambodia", "state of cambodia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567602, 0, "Kingdom of Cambodia", "kingdom of cambodia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567603, 0, "République khmère", "republique khmere", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567604, 0, "Cambotja", "cambotja", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567605, 0, "Kambodscha", "kambodscha", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567606, 0, "Cambogia", "cambogia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567607, 0, "Cam Bot", "cam bot", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567608, 0, "Kampuchea (Coalition Government, 1983- )", "kampuchea coalition government 1983", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567609, 0, "Royal Government of Cambodia", "royal government of cambodia", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567610, 0, "Democratic Kampuchea", "democratic kampuchea", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567611, 0, "Kâmpŭchéa Prâchéathĭpâteyy", "kampuchea pracheathipateyy", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567612, 0, "Chien-pʻu-chai", "chien pu chai", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567613, 0, "Kamphūchā", "kamphucha", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567614, 0, "Cambodja", "cambodja", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567615, 0, "Kamboja", "kamboja", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567616, 0, "Kampuchea", "kampuchea", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567617, 0, "Preăhréachéanachâkr Kâmpŭchéa", "preahreacheanachakr kampuchea", 5, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2567618, 0, "Democratic Cambodia", "democratic cambodia", 5, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1397094, 624823, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1397094, 1127010, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661897, 2567587, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661898, 2567588, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661899, 2567589, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661900, 2567590, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661901, 2567591, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661902, 2567592, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661903, 2567593, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661904, 2567594, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661905, 2567595, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661906, 2567596, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661907, 2567597, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661908, 2567598, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661909, 2567599, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661910, 2567600, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661911, 2567601, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661912, 2567602, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661913, 2567603, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661914, 2567604, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661915, 2567605, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661916, 2567606, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661917, 2567607, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661918, 2567608, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661919, 2567609, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661920, 2567610, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661921, 2567611, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661922, 2567612, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661923, 2567613, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661924, 2567614, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661925, 2567615, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661926, 2567616, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661927, 2567617, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661928, 2567618, 1397094, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1661929, 1183744, 1397094, 3, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661897, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661898, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661899, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661900, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661901, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661902, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661903, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661904, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661905, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661906, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661907, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661908, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661909, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661910, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661911, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661912, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661913, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661914, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661915, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661916, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661917, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661918, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661919, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661920, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661921, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661922, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661923, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661924, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661925, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661926, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661927, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661928, 1127010);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1661929, 1127010);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (885921, 1, "sh 93007047", "Electronic books", 914280, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1550019, 0, "Books", "books", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1887436, 0, "Electronic publications", "electronic publications", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997592, 0, "Electronic books", "electronic books", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997593, 0, "Online books", "online books", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997594, 0, "Digital books", "digital books", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997595, 0, "E-books", "e books", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997596, 0, "Books in machine-readable form", "books in machine readable form", 4, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (1997597, 0, "Ebooks", "ebooks", 4, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1997592, 885921, 1);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1997592, 885926, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (1997592, 6844924, 0);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301108, 1997593, 1997592, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301109, 1997594, 1997592, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301110, 1997595, 1997592, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301111, 1997596, 1997592, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301112, 1997597, 1997592, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301113, 1550019, 1997592, 3, "Narrower Term");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1301114, 1887436, 1997592, 3, "Narrower Term");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301108, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301109, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301110, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301111, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301112, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301113, 885921);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1301114, 885921);


-- TOCTest data
INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (5390754, 0, "nr2001039107", "Cornell University. College of Veterinary Medicine. Flower-Sprecher Veterinary Library", 5593099, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10293879, 0, "New York State College of Veterinary Medicine. Flower Veterinary Library", "new york state college of veterinary medicine flower veterinary library", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10293883, 0, "Cornell University. College of Veterinary Medicine. Flower-Sprecher Veterinary Library", "cornell university college of veterinary medicine flower sprecher veterinary library", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10293899, 0, "Cornell University. College of Veterinary Medicine. Library", "cornell university college of veterinary medicine library", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10293900, 0, "Roswell P. Flower-Isidor I. and Sylvia M. Sprecher Library and Learning Resources Center", "roswell p flower isidor i and sylvia m sprecher library and learning resources center", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (10293901, 0, "Flower-Sprecher Veterinary Library", "flower sprecher veterinary library", 1, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (10293883, 5390745, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (10293883, 5390754, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5441038, 10293899, 10293883, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5441039, 10293900, 10293883, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5441040, 10293901, 10293883, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5441041, 10293879, 10293883, 3, "Later Heading");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5441038, 5390754);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5441039, 5390754);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5441040, 5390754);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5441041, 5390754);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (899021, 0, "n  87139570", "Salmon, D. E.", 928121, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2027395, 0, "Salmon, D. E.", "salmon d e", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2027396, 0, "Salmon, Daniel Elmer", "salmon daniel elmer", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (2027397, 0, "Salmon, D. E. (Daniel Elmer)", "salmon d e daniel elmer", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (2027395, 899021, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1319462, 2027396, 2027395, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (1319463, 2027397, 2027395, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1319462, 899021);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (1319463, 899021);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (346500, 0, "n  84806477", "Thurston, Robert Henry, 1839-1903", 348942, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (771938, 0, "Thurston, Robert Henry, 1839-1903", "thurston robert henry 1839 1903", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (771939, 0, "Thurston, R. H. (Robert Henry), 1839-1903", "thurston r h robert henry 1839 1903", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (771938, 346500, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (438507, 771939, 771938, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (438507, 346500);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (4878952, 0, "nr 99030039", "E. & F.N. Spon", 5067695, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (9391968, 0, "E. & F.N. Spon", "e fn spon 2&", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (9391969, 0, "E. and F.N. Spon", "e and fn spon", 1, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (9391968, 4878952, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (5016261, 9391969, 9391968, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (5016261, 4878952);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (8557464, 0, "n 2011057129", "Helgi Haraldsson, 1938-", 8832048, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (15540893, 0, "Helgi Haraldsson, 1938-", "helgi haraldsson 1938", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (15540894, 0, "Khelʹgi Kharalʹdsson, 1938-", "khelgi kharaldsson 1938", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (15540895, 0, "Haraldsson, Helgi, 1938-", "haraldsson helgi 1938", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (15540893, 8557464, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (7772870, 15540894, 15540893, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (7772871, 15540895, 15540893, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (7772870, 8557464);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (7772871, 8557464);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (176495, 0, "n  81072606", "Rosenthal, Manuel, 1904-2003", 177891, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (385813, 0, "Rosenthal, Manuel, 1904-2003", "rosenthal manuel 1904 2003", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (385814, 0, "Rosenthal, Emmanuel, 1904-2003", "rosenthal emmanuel 1904 2003", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (385813, 176495, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (218163, 385814, 385813, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (218163, 176495);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (7938881, 0, "no2009190394", "Fa lü chu ban she. Fa gui chu ban fen she", 8205082, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (14540121, 0, "Fa lü chu ban she. Fa gui chu ban fen she", "fa lu chu ban she fa gui chu ban fen she", 1, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (14540122, 0, "法律出版社. 法规出版分社.", "法律出版社 法规出版分社", 1, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (14540121, 7938881, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (7349248, 14540122, 14540121, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (7349248, 7938881);

INSERT INTO authority (id, source, nativeId, nativeHeading, voyagerId, undifferentiated) VALUES (4111039, 0, "nr 94002689", "Ko, Dorothy, 1957-", 4290234, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (8042916, 0, "Ko, Dorothy, 1957-", "ko dorothy 1957", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (8042917, 0, "高彦颐, 1957-", "高彦颐 1957", 0, 0, 0, 0);
INSERT INTO heading (id, parent_id, heading, sort, heading_type, works_by, works_about, works) VALUES (8042918, 0, "Gao, Yanyi, 1957-", "gao yanyi 1957", 0, 0, 0, 0);
INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (8042916, 4111039, 1);
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (4378168, 8042917, 8042916, 1, "");
INSERT INTO reference (id, from_heading, to_heading, ref_type, ref_desc) VALUES (4378169, 8042918, 8042916, 1, "");
INSERT INTO authority2reference (reference_id, authority_id) VALUES (4378168, 4111039);
INSERT INTO authority2reference (reference_id, authority_id) VALUES (4378169, 4111039);

CREATE TABLE `syndeticsData` (
  `isbn` varchar(14) NOT NULL,
  `marc` mediumtext NOT NULL,
  KEY `isbn` (`isbn`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO syndeticsData (isbn, marc) VALUES ("9780691202181", "<USMARC><Leader/><VarFlds><VarCFlds><Fld001>72158254</Fld001><Fld005>20200919000000.0</Fld005><Fld008/></VarCFlds><VarDFlds><NumbCode><Fld020 I1='BLANK' I2='BLANK'><a>9780691202181</a></Fld020></NumbCode><MainEnty><Fld100 I1='BLANK' I2='BLANK'><a>Roberts, Sean R.</a></Fld100></MainEnty><Titles><Fld245 I1='BLANK' I2='BLANK'><a>The\War on the Uyghurs: China's Internal Campaign Against a Muslim Minority</a></Fld245></Titles><SSIFlds><Fld970 I1='0' I2='1'><t>Map: Xinjiang Uyghur Autonomous Region</t><p>p. viii</p></Fld970><Fld970 I1='0' I2='1'><t>Foreword</t><p>p. ix</p><c>Ben Emmerson</c></Fld970><Fld970 I1='0' I2='1'><t>Preface</t><p>p. xii</p></Fld970><Fld970 I1='0' I2='1'><t>Introduction</t><p>p. 1</p></Fld970><Fld970 I1='1' I2='1'><l>1</l><t>Colonialism, 1759-2001</t><p>p. 21</p></Fld970><Fld970 I1='1' I2='1'><l>2</l><t>How the Uyghurs became a 'terrorist threat'</t><p>p. 63</p></Fld970><Fld970 I1='1' I2='1'><l>3</l><t>Myths and realities of the alleged 'terrorist threat' associated with Uyghurs</t><p>p. 97</p></Fld970><Fld970 I1='1' I2='1'><l>4</l><t>Colonialism meets counterterrorism, 2002-2012</t><p>p. 131</p></Fld970><Fld970 I1='1' I2='1'><l>5</l><t>The self-fulfilling prophecy and the 'People's War on Terror,' 2013-2016</t><p>p. 161</p></Fld970><Fld970 I1='1' I2='1'><l>6</l><t>Cultural genocide, 2017-2020</t><p>p. 199</p></Fld970><Fld970 I1='0' I2='1'><t>Conclusion</t><p>p. 236</p></Fld970><Fld970 I1='0' I2='1'><t>A note on methodology</t><p>p. 252</p></Fld970><Fld970 I1='0' I2='1'><t>Transliteration and place names</t><p>p. 257</p></Fld970><Fld970 I1='0' I2='1'><t>List of figures</t><p>p. 259</p></Fld970><Fld970 I1='0' I2='1'><t>List of abbreviations</t><p>p. 260</p></Fld970><Fld970 I1='0' I2='1'><t>Acknowledgments</t><p>p. 262</p></Fld970><Fld970 I1='0' I2='1'><t>Notes</t><p>p. 266</p></Fld970><Fld970 I1='0' I2='1'><t>Index</t><p>p. 301</p></Fld970><Fld997 I1='BLANK' I2='BLANK'></Fld997></SSIFlds></VarDFlds></VarFlds></USMARC>");


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

-- HathiLinksTest data
INSERT INTO raw_hathi (Volume_Identifier,Access,Rights,UofM_Record_Number,Enum_Chrono,Source,Source_Inst_Record_Number,OCLC_Numbers,ISBNs,ISSNs,LCCNs,Title,Imprint,Rights_determine_reason_code,Date_Last_Update,Gov_Doc,Pub_Date,Pub_Place,Language,Bib_Format,Digitization_Agent_code,Content_provider_code,Responsible_Entity_code,Collection_code,update_file_name,record_counter) VALUES ("coo.31924090258827","allow","pd","008595162","","COO","318","2534902","","","51008544","Birds in the bush, by Bradford Torrey.","Houghton, Mifflin and Company, 1885.","bib","2010-12-13 20:30:49","0","1885","mau","eng","BK","COO","cornell","cornell","google","./hathifiles/hathi_full_20221101.txt","10815922");
INSERT INTO raw_hathi (Volume_Identifier,Access,Rights,UofM_Record_Number,Enum_Chrono,Source,Source_Inst_Record_Number,OCLC_Numbers,ISBNs,ISSNs,LCCNs,Title,Imprint,Rights_determine_reason_code,Date_Last_Update,Gov_Doc,Pub_Date,Pub_Place,Language,Bib_Format,Digitization_Agent_code,Content_provider_code,Responsible_Entity_code,Collection_code,update_file_name,record_counter) VALUES ("nyp.33433011014200","allow","pd","008595162","","NYP","b13556394x","2534902","","","51008544","Birds in the bush, by Bradford Torrey.","Houghton, Mifflin and Company, 1885.","bib","2010-09-27 19:31:05","0","1885","mau","eng","BK","NYP","nypl","nypl","google","./hathifiles/hathi_full_20221101.txt","10815923");
INSERT INTO raw_hathi (Volume_Identifier,Access,Rights,UofM_Record_Number,Enum_Chrono,Source,Source_Inst_Record_Number,OCLC_Numbers,ISBNs,ISSNs,LCCNs,Title,Imprint,Rights_determine_reason_code,Date_Last_Update,Gov_Doc,Pub_Date,Pub_Place,Language,Bib_Format,Digitization_Agent_code,Content_provider_code,Responsible_Entity_code,Collection_code,update_file_name,record_counter) VALUES ("coo.31924005214295","allow","pd","100174680","no.25-30","COO","178","2124566","","","17013088","Opinions of the attorneys general and judgments of the Supreme court and Court of claims of the United States relating to the controversy over neutral rights between the United States and France, 1797-1800.","The Endowment, 1917.","bib","2014-07-07 03:25:28","0","1917","dcu","eng","BK","COO","cornell","cornell","google","./hathifiles/hathi_full_20221101.txt","14673503");
INSERT INTO raw_hathi (Volume_Identifier,Access,Rights,UofM_Record_Number,Enum_Chrono,Source,Source_Inst_Record_Number,OCLC_Numbers,ISBNs,ISSNs,LCCNs,Title,Imprint,Rights_determine_reason_code,Date_Last_Update,Gov_Doc,Pub_Date,Pub_Place,Language,Bib_Format,Digitization_Agent_code,Content_provider_code,Responsible_Entity_code,Collection_code,update_file_name,record_counter) VALUES ("coo1.ark:/13960/t20c5hb54","allow","pd","100763896","","COO","1460864","63972144","","","","Agricultural co-operation.","Democrat print. co., 1912.","bib","2015-07-20 12:26:18","0","1912","wiu","eng","BK","COO","cornell","cornell","cornell-ms","./hathifiles/hathi_full_20221101.txt","15517707");
INSERT INTO raw_hathi (Volume_Identifier,Access,Rights,UofM_Record_Number,Source,Source_Inst_Record_Number,OCLC_Numbers,Title,Imprint,Rights_determine_reason_code,Date_Last_Update,Gov_Doc,Pub_Date,Pub_Place,Language,Bib_Format,Digitization_Agent_code,Content_provider_code,Responsible_Entity_code,Collection_code,update_file_name,record_counter) VALUES ("coo.31924000030001","allow","pd","102756782","COO","10519,939641","31301237","The practical pigeon keeper.","Cassell,Petter,Galpin & Co. [1882?]","bib","2021-01-20 17:19:59","0","1882","enk","eng","BK","COO","cornell","cornell","cornell","./hathifiles/hathi_full_20221101.txt","17573781");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("coo.31924090258827", "318");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("nyp.33433011014200", "b13556394x");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("coo.31924005214295", "178");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("coo1.ark:/13960/t20c5hb54", "1460864");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("coo.31924000030001", "10519");
INSERT INTO volume_to_source_inst_rec_num (Volume_Identifier, Source_Inst_Record_Number) VALUES ("coo.31924000030001", "939641");

CREATE TABLE `classification` (
  `low_letters` char(3) NOT NULL,
  `high_letters` char(3) NOT NULL,
  `low_numbers` float(10,4) NOT NULL,
  `high_numbers` float(10,4) NOT NULL,
  `label` varchar(256) CHARACTER SET utf8 NOT NULL,
  KEY `low_letters` (`low_letters`,`high_letters`,`low_numbers`,`high_numbers`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- utilities CallNumberTest data
INSERT INTO classification (low_letters, high_letters, low_numbers, high_numbers, label) VALUES ("Q", "QZZ", 0.0000, 100000.0000, "Q - Science");
INSERT INTO classification (low_letters, high_letters, low_numbers, high_numbers, label) VALUES ("QA", "QA", 0.0000, 100000.0000, "QA - Mathematics");
INSERT INTO classification (low_letters, high_letters, low_numbers, high_numbers, label) VALUES ("QA", "QA", 440.0000, 699.9999, "QA440-699 - Geometry.  Trigonometry.  Topology");
